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

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 3 DD-8
/// Verifies: an existing v4 database rekeys explicit claims once while inferred keys stay fixed.
#[tokio::test]
async fn v4_database_rekeys_only_explicit_rows_and_reopens_idempotently() {
    let data_root = TempDir::new().expect("memory data root");
    let memory_root = data_root.path().join("memory");
    fs::create_dir_all(&memory_root).expect("create memory root");
    let database_path = memory_root.join("memory.sqlite3");
    let connection = Connection::open(&database_path).expect("create v4 memory database");
    connection
        .execute_batch(
            "CREATE TABLE memory_schema_meta (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL);
             INSERT INTO memory_schema_meta VALUES ('schema_version', '4');
             CREATE TABLE memory_entries (
                 entry_id TEXT PRIMARY KEY NOT NULL, scope_type TEXT NOT NULL,
                 scope_id TEXT NOT NULL, kind TEXT NOT NULL, normalized_key TEXT NOT NULL,
                 body TEXT NOT NULL, origin TEXT NOT NULL, state TEXT NOT NULL,
                 created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                 last_recalled_at TEXT, replacement_entry_id TEXT, expires_at TEXT
             );
             CREATE UNIQUE INDEX memory_entries_scope_key
                 ON memory_entries (scope_type, scope_id, normalized_key);
             CREATE TABLE memory_candidates (
                 candidate_id TEXT PRIMARY KEY NOT NULL, scope_type TEXT NOT NULL,
                 scope_id TEXT NOT NULL, kind TEXT NOT NULL, normalized_key TEXT NOT NULL,
                 body TEXT NOT NULL, origin TEXT NOT NULL,
                 source_session_id TEXT NOT NULL, validation_outcome TEXT,
                 retention_until TEXT NOT NULL, created_at TEXT NOT NULL
             );
             CREATE TABLE memory_evidence (
                 evidence_id TEXT PRIMARY KEY NOT NULL, entry_id TEXT NOT NULL,
                 session_id TEXT NOT NULL, turn_id TEXT, source_user_item_id TEXT,
                 observed_at TEXT NOT NULL, source_watermark TEXT NOT NULL,
                 FOREIGN KEY(entry_id) REFERENCES memory_entries(entry_id)
             );
             CREATE TABLE memory_revocations (
                 revocation_id TEXT PRIMARY KEY NOT NULL, scope_type TEXT NOT NULL,
                 scope_id TEXT NOT NULL, normalized_key TEXT NOT NULL,
                 revoked_at TEXT NOT NULL, restored_at TEXT
             );
             CREATE UNIQUE INDEX memory_revocations_scope_identity
                 ON memory_revocations (scope_type, scope_id, normalized_key);
             CREATE TABLE memory_jobs (
                 job_id TEXT PRIMARY KEY NOT NULL,
                 job_kind TEXT NOT NULL DEFAULT 'source_scan',
                 job_key TEXT NOT NULL DEFAULT '',
                 source_session_id TEXT NOT NULL, source_watermark TEXT NOT NULL,
                 state TEXT NOT NULL, attempt_count INTEGER NOT NULL DEFAULT 0,
                 lease_until TEXT, lease_owner TEXT, claimed_at TEXT, retry_at TEXT,
                 error_class TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                 UNIQUE(source_session_id, source_watermark)
             );
             CREATE UNIQUE INDEX memory_jobs_kind_key ON memory_jobs (job_kind, job_key);
             CREATE TABLE memory_scope_state (
                 scope_type TEXT NOT NULL, scope_id TEXT NOT NULL,
                 projection_revision INTEGER NOT NULL DEFAULT 0,
                 ignore_sources_before TEXT, last_rebuild_at TEXT,
                 PRIMARY KEY(scope_type, scope_id)
             );
             CREATE VIRTUAL TABLE memory_entries_fts USING fts5(
                 entry_id UNINDEXED, normalized_key, body
             );
             INSERT INTO memory_entries
                 (entry_id, scope_type, scope_id, kind, normalized_key, body, origin,
                  state, created_at, updated_at)
             VALUES
                 ('explicit-v4', 'user', 'user', 'preference', 'i prefer dark mode',
                  'Please remember that I prefer dark mode', 'explicit_user', 'active',
                  '2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z'),
                 ('inferred-v4', 'user', 'user', 'fact', 'inferred-opaque-key',
                  'An inferred claim', 'inferred_session', 'active',
                  '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
             INSERT INTO memory_evidence VALUES
                 ('evidence-v4', 'explicit-v4', 'session-v4', NULL, NULL,
                  '2026-01-02T00:00:00Z', '2026-01-02T00:00:00Z');
             INSERT INTO memory_revocations VALUES
                 ('revocation-v4', 'user', 'user', 'i prefer dark mode',
                  '2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z');
             INSERT INTO memory_entries_fts
                 SELECT entry_id, normalized_key, body FROM memory_entries;
             CREATE TRIGGER reject_v5_rekey BEFORE UPDATE ON memory_entries
             BEGIN SELECT RAISE(ABORT, 'test migration rollback'); END;",
        )
        .expect("seed v4 storage");
    drop(connection);

    assert!(MemoryRuntime::open(memory_root.clone(), enabled_config()).is_err());
    let connection = Connection::open(&database_path).expect("inspect rolled back v4 database");
    let before_retry: (String, String, String) = connection
        .query_row(
            "SELECT
                 (SELECT value FROM memory_schema_meta WHERE key = 'schema_version'),
                 (SELECT normalized_key FROM memory_entries WHERE entry_id = 'explicit-v4'),
                 (SELECT normalized_key FROM memory_revocations WHERE revocation_id = 'revocation-v4')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("read rolled back data");
    assert_eq!(
        before_retry,
        (
            "4".to_string(),
            "i prefer dark mode".to_string(),
            "i prefer dark mode".to_string()
        )
    );
    connection
        .execute_batch("DROP TRIGGER reject_v5_rekey;")
        .expect("allow retry");
    drop(connection);

    let runtime = MemoryRuntime::open(memory_root.clone(), enabled_config())
        .expect("upgrade v4 memory database");
    let first_list = list(&runtime, MemoryScope::User, data_root.path()).await;
    assert_eq!(first_list.len(), 2);
    assert_eq!(first_list[0].entry_id.as_str(), "explicit-v4");
    assert_eq!(
        first_list[0].normalized_key,
        "Please remember that I prefer dark mode"
    );
    assert_eq!(first_list[1].normalized_key, "inferred-opaque-key");
    drop(runtime);

    let connection = Connection::open(&database_path).expect("inspect v5 database");
    let stored: (String, String, i64, i64) = connection
        .query_row(
            "SELECT
                 (SELECT value FROM memory_schema_meta WHERE key = 'schema_version'),
                 (SELECT normalized_key FROM memory_revocations WHERE revocation_id = 'revocation-v4'),
                 (SELECT COUNT(*) FROM memory_evidence WHERE entry_id = 'explicit-v4'),
                 (SELECT COUNT(*) FROM memory_entries_fts WHERE memory_entries_fts MATCH 'dark')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("read migrated v5 state");
    assert_eq!(
        stored,
        (
            "5".to_string(),
            "Please remember that I prefer dark mode".to_string(),
            1,
            1
        )
    );
    connection
        .execute_batch(
            "CREATE TRIGGER reject_second_rekey BEFORE UPDATE ON memory_revocations
             BEGIN SELECT RAISE(ABORT, 'v5 startup must not migrate again'); END;
             CREATE TRIGGER reject_second_version_write BEFORE UPDATE ON memory_schema_meta
             BEGIN SELECT RAISE(ABORT, 'v5 startup must not rewrite version'); END;",
        )
        .expect("guard against a second migration");
    drop(connection);
    assert!(
        fs::read_to_string(memory_root.join("user").join("MEMORY.md"))
            .expect("read regenerated projection")
            .contains("Please remember that I prefer dark mode")
    );

    let reopened =
        MemoryRuntime::open(memory_root, enabled_config()).expect("reopen migrated database");
    assert_eq!(
        list(&reopened, MemoryScope::User, data_root.path()).await,
        first_list
    );
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 3 DD-4, DD-8
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

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-4, DD-8, DD-9
/// Verifies: v4-to-v5 migration merges legacy keys without reviving a revoked identity.
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
            text: "I prefer compact responses".to_string(),
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
            rusqlite::params!["legacy-original-claim", oldest.entry_id.as_str(),],
        )
        .expect("restore legacy normalized key");
    connection
        .execute(
            "UPDATE memory_entries SET last_recalled_at = '2031-01-01T00:00:00Z'
             WHERE entry_id = ?1",
            [oldest.entry_id.as_str()],
        )
        .expect("record recall on canonical entry");
    connection
        .execute(
            "INSERT INTO memory_entries (
                 entry_id, scope_type, scope_id, kind, normalized_key, body,
                 origin, state, created_at, updated_at
             ) VALUES (?1, 'user', ?2, 'fact', ?3, ?4,
                       'explicit_user', 'active', ?5, ?5)",
            rusqlite::params![
                "legacy-duplicate",
                oldest.scope_id,
                "legacy-duplicate-claim",
                "i prefer compact responses!",
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
                "legacy-retired-claim",
                "I prefer compact responses.",
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
                  'legacy-original-claim',
                  '2027-01-01T00:00:00Z', '2028-01-01T00:00:00Z'),
                 ('legacy-revocation-2', 'user', 'user',
                  'legacy-duplicate-claim',
                  '2035-01-01T00:00:00Z', NULL);",
        )
        .expect("insert converging legacy revocations");
    connection
        .execute_batch(
            "DELETE FROM memory_entries_fts;
             INSERT INTO memory_entries_fts (entry_id, normalized_key, body)
             SELECT entry_id, normalized_key, body FROM memory_entries;
             UPDATE memory_schema_meta SET value = '4' WHERE key = 'schema_version';",
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
    expected.body = "i prefer compact responses!".to_string();
    expected.kind = MemoryKind::Fact;
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
    let stored: (String, i64, i64, Option<String>) = connection
        .query_row(
            "SELECT
                 (SELECT value FROM memory_schema_meta WHERE key = 'schema_version'),
                 (SELECT COUNT(*) FROM memory_entries),
                 (SELECT COUNT(*) FROM memory_entries_fts
                  WHERE memory_entries_fts MATCH 'compact'),
                 (SELECT last_recalled_at FROM memory_entries
                  WHERE entry_id = ?1)",
            [expected_entries[0].entry_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("read migrated storage state");
    assert_eq!(
        stored,
        (
            "5".to_string(),
            2,
            0,
            Some("2031-01-01T00:00:00Z".to_string())
        )
    );
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
    assert!(projection.contains("i prefer compact responses!"));
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
            text: "I prefer restored migrations".to_string(),
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
            rusqlite::params!["legacy-restored-claim", remembered.entry_id.as_str(),],
        )
        .expect("restore legacy normalized key");
    connection
        .execute(
            "INSERT INTO memory_revocations (
                 revocation_id, scope_type, scope_id, normalized_key, revoked_at, restored_at
             ) VALUES (?1, 'user', 'user', ?2, ?3, ?4)",
            rusqlite::params![
                "legacy-restored-revocation",
                "legacy-restored-claim",
                "2027-01-01T00:00:00Z",
                "2030-01-01T00:00:00Z",
            ],
        )
        .expect("insert restored legacy revocation");
    connection
        .execute(
            "UPDATE memory_schema_meta SET value = '4' WHERE key = 'schema_version'",
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
