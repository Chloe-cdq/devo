use std::fs;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use devo_core::MemoryConfig;
use devo_protocol::native::ids::{ItemId, MemoryEntryId};
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::{
    MemoryEntry, MemoryKind, MemoryOrigin, MemoryProvenance, MemoryScope, MemoryState,
};
use devo_protocol::{SessionId, TurnId};
use devo_server::memory::MemorySourceContext;
use devo_server::memory::{
    ListMemoryRequest, MemoryCommand, MemoryCommandResult, MemoryForgetRequest,
    MemoryForgetSelector, MemoryForgetSource, MemoryRememberRequest, MemoryRuntime,
    ProjectMemorySession, ProjectMemorySessionActivity,
};
use pretty_assertions::assert_eq;
use rusqlite::Connection;
use tempfile::TempDir;

#[path = "../src/memory/test_support.rs"]
mod memory_test_support;

fn enabled_config() -> MemoryConfig {
    MemoryConfig {
        enabled: true,
        ..MemoryConfig::default()
    }
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-8, DD-9
/// Verifies: explicit restore reuses a retired legacy inferred entry and canonicalizes its identity.
#[tokio::test]
async fn explicit_restore_reuses_retired_legacy_inferred_entry() {
    let data_root = TempDir::new().expect("memory data root");
    let memory_root = data_root.path().join("memory");
    let runtime =
        MemoryRuntime::open(memory_root.clone(), enabled_config()).expect("create memory runtime");
    drop(runtime);

    let created_at = "2026-01-01T00:00:00Z";
    let revoked_at = "2026-01-02T00:00:00Z";
    let database_path = memory_root.join("memory.sqlite3");
    let connection = Connection::open(&database_path).expect("open memory database");
    connection
        .execute(
            "INSERT INTO memory_entries (
                 entry_id, scope_type, scope_id, kind, normalized_key, body,
                 origin, state, created_at, updated_at
             ) VALUES (?1, 'user', 'user', 'preference', 'use apikey',
                       'Use API_KEY', 'inferred_session', 'retired', ?2, ?3)",
            rusqlite::params!["legacy-inferred-entry", created_at, revoked_at],
        )
        .expect("insert retired legacy inferred entry");
    connection
        .execute(
            "INSERT INTO memory_revocations (
                 revocation_id, scope_type, scope_id, normalized_key, revoked_at, restored_at
             ) VALUES ('legacy-inferred-revocation', 'user', 'user', 'use apikey', ?1, NULL)",
            [revoked_at],
        )
        .expect("insert legacy inferred revocation");
    drop(connection);

    let runtime =
        MemoryRuntime::open(memory_root, enabled_config()).expect("reopen memory runtime");
    let restored = match runtime
        .execute_command(MemoryCommand::Remember(MemoryRememberRequest {
            text: "Use API_KEY".to_owned(),
            scope: MemoryScope::User,
            kind: Some(MemoryKind::Preference),
            source: memory_test_support::test_source(
                Some("user-item-1"),
                "session-1",
                Some("turn-1"),
                PathBuf::new(),
            ),
        }))
        .await
        .expect("restore legacy inferred entry")
    {
        MemoryCommandResult::Remember(entry) => entry,
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::PreparedForget(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_)
        | MemoryCommandResult::Search(_) => panic!("expected remembered entry"),
    };
    let expected = MemoryEntry {
        entry_id: MemoryEntryId::from_string("legacy-inferred-entry".to_owned()),
        scope: MemoryScope::User,
        scope_id: "user".to_owned(),
        kind: MemoryKind::Preference,
        normalized_key: "Use API_KEY".to_owned(),
        body: "Use API_KEY".to_owned(),
        origin: MemoryOrigin::ExplicitUser,
        state: MemoryState::Restored,
        created_at: DateTime::parse_from_rfc3339(created_at)
            .expect("created timestamp")
            .with_timezone(&Utc),
        updated_at: restored.updated_at,
        replacement_entry_id: None,
        provenance: restored.provenance.clone(),
    };
    assert_eq!(restored, expected);

    let listed = match runtime
        .execute_command(MemoryCommand::List(ListMemoryRequest {
            scope: Some(MemoryScope::User),
            workspace_root: PathBuf::new(),
            ..ListMemoryRequest::default()
        }))
        .await
        .expect("list restored identity")
    {
        MemoryCommandResult::List(page) => page,
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::PreparedForget(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::Search(_) => panic!("expected memory list"),
    };
    assert_eq!(
        listed,
        Page {
            data: vec![expected],
            next_cursor: None,
        }
    );
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-8
/// Verifies: canonical and legacy inferred aliases merge into the canonical stable identity.
#[tokio::test]
async fn explicit_remember_merges_existing_canonical_and_legacy_aliases() {
    let data_root = TempDir::new().expect("memory data root");
    let memory_root = data_root.path().join("memory");
    let runtime =
        MemoryRuntime::open(memory_root.clone(), enabled_config()).expect("create memory runtime");
    drop(runtime);

    let database_path = memory_root.join("memory.sqlite3");
    let connection = Connection::open(&database_path).expect("open memory database");
    connection
        .execute_batch(
            "INSERT INTO memory_entries (
                 entry_id, scope_type, scope_id, kind, normalized_key, body,
                 origin, state, created_at, updated_at
             ) VALUES
                 ('canonical-entry', 'user', 'user', 'preference', 'Use API_KEY',
                  'Use API_KEY', 'explicit_user', 'active',
                  '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
                 ('legacy-entry', 'user', 'user', 'preference', 'use apikey',
                  'Use API_KEY', 'inferred_session', 'active',
                  '2026-01-02T00:00:00Z', '2026-01-02T00:00:00Z'),
                 ('lineage-entry', 'user', 'user', 'fact', 'prior identity',
                  'Prior identity', 'explicit_user', 'retired',
                  '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z');
             UPDATE memory_entries
                 SET replacement_entry_id = 'legacy-entry'
                 WHERE entry_id = 'lineage-entry';
             INSERT INTO memory_evidence (
                 evidence_id, entry_id, session_id, turn_id, source_user_item_id,
                 observed_at, source_watermark
             ) VALUES
                 ('canonical-evidence', 'canonical-entry', 'canonical-session', NULL, NULL,
                  '2026-01-01T00:00:00Z', 'canonical-watermark'),
                 ('legacy-evidence', 'legacy-entry', 'legacy-session', NULL, NULL,
                  '2026-01-02T00:00:00Z', 'legacy-watermark');
             INSERT INTO memory_entries_fts (entry_id, normalized_key, body)
                 SELECT entry_id, normalized_key, body FROM memory_entries;",
        )
        .expect("insert canonical and legacy aliases");
    drop(connection);

    let runtime =
        MemoryRuntime::open(memory_root, enabled_config()).expect("reopen memory runtime");
    let remembered = match runtime
        .execute_command(MemoryCommand::Remember(MemoryRememberRequest {
            text: "Use API_KEY".to_owned(),
            scope: MemoryScope::User,
            kind: Some(MemoryKind::Preference),
            source: memory_test_support::test_source(
                Some("user-item-1"),
                "session-1",
                Some("turn-1"),
                PathBuf::new(),
            ),
        }))
        .await
        .expect("remember canonical identity")
    {
        MemoryCommandResult::Remember(entry) => entry,
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::PreparedForget(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_)
        | MemoryCommandResult::Search(_) => panic!("expected remembered entry"),
    };
    let expected = MemoryEntry {
        entry_id: MemoryEntryId::from_string("canonical-entry".to_owned()),
        scope: MemoryScope::User,
        scope_id: "user".to_owned(),
        kind: MemoryKind::Preference,
        normalized_key: "Use API_KEY".to_owned(),
        body: "Use API_KEY".to_owned(),
        origin: MemoryOrigin::ExplicitUser,
        state: MemoryState::Active,
        created_at: DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("created timestamp")
            .with_timezone(&Utc),
        updated_at: remembered.updated_at,
        replacement_entry_id: None,
        provenance: vec![
            MemoryProvenance {
                source_session_id: Some("canonical-session".to_owned()),
                source_turn_id: None,
                source_user_item_id: None,
            },
            MemoryProvenance {
                source_session_id: Some("legacy-session".to_owned()),
                source_turn_id: None,
                source_user_item_id: None,
            },
            MemoryProvenance {
                source_session_id: Some(
                    SessionId::from(memory_test_support::deterministic_uuid("session-1"))
                        .to_string(),
                ),
                source_turn_id: Some(
                    TurnId::from(memory_test_support::deterministic_uuid("turn-1")).to_string(),
                ),
                source_user_item_id: Some(ItemId::from_string(format!(
                    "item_{:032x}",
                    memory_test_support::deterministic_uuid("user-item-1").as_u128()
                ))),
            },
        ],
    };
    assert_eq!(remembered, expected);

    let listed = match runtime
        .execute_command(MemoryCommand::List(ListMemoryRequest {
            scope: Some(MemoryScope::User),
            workspace_root: PathBuf::new(),
            ..ListMemoryRequest::default()
        }))
        .await
        .expect("list canonical identity")
    {
        MemoryCommandResult::List(page) => page,
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::PreparedForget(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::Search(_) => panic!("expected memory list"),
    };
    assert_eq!(
        listed,
        Page {
            data: vec![
                expected,
                MemoryEntry {
                    entry_id: MemoryEntryId::from_string("lineage-entry".to_owned()),
                    scope: MemoryScope::User,
                    scope_id: "user".to_owned(),
                    kind: MemoryKind::Fact,
                    normalized_key: "prior identity".to_owned(),
                    body: "Prior identity".to_owned(),
                    origin: MemoryOrigin::ExplicitUser,
                    state: MemoryState::Retired,
                    created_at: DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
                        .expect("lineage created timestamp")
                        .with_timezone(&Utc),
                    updated_at: DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
                        .expect("lineage updated timestamp")
                        .with_timezone(&Utc),
                    replacement_entry_id: Some(MemoryEntryId::from_string(
                        "canonical-entry".to_owned(),
                    )),
                    provenance: Vec::new(),
                },
            ],
            next_cursor: None,
        }
    );
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-8
/// Verifies: a lossy legacy-key collision cannot merge incompatible structured claims.
#[tokio::test]
async fn explicit_remember_preserves_incompatible_legacy_key_collision() {
    let data_root = TempDir::new().expect("memory data root");
    let memory_root = data_root.path().join("memory");
    let runtime =
        MemoryRuntime::open(memory_root.clone(), enabled_config()).expect("create memory runtime");
    drop(runtime);

    let database_path = memory_root.join("memory.sqlite3");
    let connection = Connection::open(&database_path).expect("open memory database");
    connection
        .execute_batch(
            "INSERT INTO memory_entries (
                 entry_id, scope_type, scope_id, kind, normalized_key, body,
                 origin, state, created_at, updated_at
             ) VALUES (
                 'legacy-entry', 'user', 'user', 'preference', 'use foo1',
                 'Use foo1', 'inferred_session', 'active',
                 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
             );
             INSERT INTO memory_entries_fts (entry_id, normalized_key, body)
                 SELECT entry_id, normalized_key, body FROM memory_entries;",
        )
        .expect("insert incompatible legacy claim");
    drop(connection);

    let runtime =
        MemoryRuntime::open(memory_root, enabled_config()).expect("reopen memory runtime");
    let remembered = match runtime
        .execute_command(MemoryCommand::Remember(MemoryRememberRequest {
            text: "Use FOO=1".to_owned(),
            scope: MemoryScope::User,
            kind: Some(MemoryKind::Preference),
            source: memory_test_support::test_source(
                Some("user-item-1"),
                "session-1",
                Some("turn-1"),
                PathBuf::new(),
            ),
        }))
        .await
        .expect("remember structured claim")
    {
        MemoryCommandResult::Remember(entry) => entry,
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::PreparedForget(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_)
        | MemoryCommandResult::Search(_) => panic!("expected remembered entry"),
    };
    let expected_remembered = MemoryEntry {
        entry_id: remembered.entry_id.clone(),
        scope: MemoryScope::User,
        scope_id: "user".to_owned(),
        kind: MemoryKind::Preference,
        normalized_key: "Use FOO=1".to_owned(),
        body: "Use FOO=1".to_owned(),
        origin: MemoryOrigin::ExplicitUser,
        state: MemoryState::Active,
        created_at: remembered.created_at,
        updated_at: remembered.updated_at,
        replacement_entry_id: None,
        provenance: remembered.provenance.clone(),
    };
    assert_eq!(remembered, expected_remembered);

    let listed = match runtime
        .execute_command(MemoryCommand::List(ListMemoryRequest {
            scope: Some(MemoryScope::User),
            workspace_root: PathBuf::new(),
            ..ListMemoryRequest::default()
        }))
        .await
        .expect("list distinct identities")
    {
        MemoryCommandResult::List(page) => page,
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::PreparedForget(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::Search(_) => panic!("expected memory list"),
    };
    assert_eq!(
        listed,
        Page {
            data: vec![
                expected_remembered,
                MemoryEntry {
                    entry_id: MemoryEntryId::from_string("legacy-entry".to_owned()),
                    scope: MemoryScope::User,
                    scope_id: "user".to_owned(),
                    kind: MemoryKind::Preference,
                    normalized_key: "use foo1".to_owned(),
                    body: "Use foo1".to_owned(),
                    origin: MemoryOrigin::InferredSession,
                    state: MemoryState::Active,
                    created_at: DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                        .expect("created timestamp")
                        .with_timezone(&Utc),
                    updated_at: DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                        .expect("updated timestamp")
                        .with_timezone(&Utc),
                    replacement_entry_id: None,
                    provenance: Vec::new(),
                },
            ],
            next_cursor: None,
        }
    );
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-8, DD-9, DD-12
/// Verifies: exact forget retires only the requested stable ID without merging a legacy neighbor.
#[tokio::test]
async fn exact_forget_preserves_stable_id_without_merging_legacy_neighbor() {
    let data_root = TempDir::new().expect("memory data root");
    let memory_root = data_root.path().join("memory");
    let runtime =
        MemoryRuntime::open(memory_root.clone(), enabled_config()).expect("create memory runtime");
    drop(runtime);

    let database_path = memory_root.join("memory.sqlite3");
    let connection = Connection::open(&database_path).expect("open memory database");
    connection
        .execute_batch(
            "INSERT INTO memory_entries (
                 entry_id, scope_type, scope_id, kind, normalized_key, body,
                 origin, state, created_at, updated_at
             ) VALUES
                 ('canonical-entry', 'user', 'user', 'preference', 'Use API_KEY',
                  'Use API_KEY', 'explicit_user', 'active',
                  '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
                 ('legacy-entry', 'user', 'user', 'preference', 'use apikey',
                  'Use API_KEY', 'inferred_session', 'active',
                  '2026-01-02T00:00:00Z', '2026-01-02T00:00:00Z');
             INSERT INTO memory_evidence (
                 evidence_id, entry_id, session_id, turn_id, source_user_item_id,
                 observed_at, source_watermark
             ) VALUES
                 ('canonical-evidence', 'canonical-entry', 'canonical-session', NULL, NULL,
                  '2026-01-01T00:00:00Z', 'canonical-watermark'),
                 ('legacy-evidence', 'legacy-entry', 'legacy-session', NULL, NULL,
                  '2026-01-02T00:00:00Z', 'legacy-watermark');
             INSERT INTO memory_entries_fts (entry_id, normalized_key, body)
                 SELECT entry_id, normalized_key, body FROM memory_entries;",
        )
        .expect("insert canonical and legacy aliases");
    drop(connection);

    let runtime =
        MemoryRuntime::open(memory_root.clone(), enabled_config()).expect("reopen memory runtime");
    let source = memory_test_support::test_source(
        /*user_item_id*/ None,
        "session-1",
        /*turn_id*/ None,
        PathBuf::new(),
    );
    let prepared = match runtime
        .execute_command(MemoryCommand::PrepareForget(MemoryForgetRequest {
            selector: MemoryForgetSelector::EntryId(MemoryEntryId::from_string(
                "legacy-entry".to_owned(),
            )),
            scope: MemoryScope::User,
            source: MemoryForgetSource {
                bound_session_id: Some(source.session_id),
                user_session_id: Some(source.session_id),
                sessions: vec![ProjectMemorySession {
                    session_id: source.session_id,
                    workspace_root: Some(source.workspace_root),
                    activity: ProjectMemorySessionActivity::Active,
                }],
            },
        }))
        .await
        .expect("prepare exact forget")
    {
        MemoryCommandResult::PreparedForget(prepared) => prepared,
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_)
        | MemoryCommandResult::Search(_) => panic!("expected prepared forget"),
    };
    let forgotten = match runtime
        .execute_command(MemoryCommand::Forget(prepared))
        .await
        .expect("forget exact legacy identity")
    {
        MemoryCommandResult::Forget(result) => result.forgotten.expect("forgotten entry"),
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::PreparedForget(_)
        | MemoryCommandResult::List(_)
        | MemoryCommandResult::Search(_) => panic!("expected forget result"),
    };
    let expected_forgotten = MemoryEntry {
        entry_id: MemoryEntryId::from_string("legacy-entry".to_owned()),
        scope: MemoryScope::User,
        scope_id: "user".to_owned(),
        kind: MemoryKind::Preference,
        normalized_key: "use apikey".to_owned(),
        body: "Use API_KEY".to_owned(),
        origin: MemoryOrigin::InferredSession,
        state: MemoryState::Retired,
        created_at: DateTime::parse_from_rfc3339("2026-01-02T00:00:00Z")
            .expect("created timestamp")
            .with_timezone(&Utc),
        updated_at: forgotten.updated_at,
        replacement_entry_id: None,
        provenance: vec![MemoryProvenance {
            source_session_id: Some("legacy-session".to_owned()),
            source_turn_id: None,
            source_user_item_id: None,
        }],
    };
    assert_eq!(forgotten, expected_forgotten);

    let expected_canonical = MemoryEntry {
        entry_id: MemoryEntryId::from_string("canonical-entry".to_owned()),
        scope: MemoryScope::User,
        scope_id: "user".to_owned(),
        kind: MemoryKind::Preference,
        normalized_key: "Use API_KEY".to_owned(),
        body: "Use API_KEY".to_owned(),
        origin: MemoryOrigin::ExplicitUser,
        state: MemoryState::Active,
        created_at: DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("created timestamp")
            .with_timezone(&Utc),
        updated_at: DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("updated timestamp")
            .with_timezone(&Utc),
        replacement_entry_id: None,
        provenance: vec![MemoryProvenance {
            source_session_id: Some("canonical-session".to_owned()),
            source_turn_id: None,
            source_user_item_id: None,
        }],
    };

    let listed = match runtime
        .execute_command(MemoryCommand::List(ListMemoryRequest {
            scope: Some(MemoryScope::User),
            workspace_root: PathBuf::new(),
            ..ListMemoryRequest::default()
        }))
        .await
        .expect("list retired identity")
    {
        MemoryCommandResult::List(page) => page,
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::PreparedForget(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::Search(_) => panic!("expected memory list"),
    };
    assert_eq!(
        listed,
        Page {
            data: vec![expected_forgotten, expected_canonical],
            next_cursor: None,
        }
    );
    let connection = Connection::open(database_path).expect("reopen memory database");
    let fts_count = connection
        .query_row("SELECT COUNT(*) FROM memory_entries_fts", [], |row| {
            row.get::<_, i64>(0)
        })
        .expect("count searchable entries");
    assert_eq!(fts_count, 1);
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-4, DD-9
/// Verifies: startup replaces a stale projection for a scope retained only by a tombstone.
#[test]
fn startup_rebuilds_tombstone_only_scope_projection() {
    let data_root = TempDir::new().expect("memory data root");
    let memory_root = data_root.path().join("memory");
    let runtime =
        MemoryRuntime::open(memory_root.clone(), enabled_config()).expect("create memory runtime");
    drop(runtime);

    let database_path = memory_root.join("memory.sqlite3");
    let connection = Connection::open(&database_path).expect("open memory database");
    connection
        .execute(
            "INSERT INTO memory_revocations (
                 revocation_id, scope_type, scope_id, normalized_key, revoked_at, restored_at
             ) VALUES ('tombstone-only', 'user', 'user', 'forgotten identity',
                       '2026-01-02T00:00:00Z', NULL)",
            [],
        )
        .expect("insert tombstone-only scope");
    drop(connection);

    let projection_path = memory_root.join("user").join("MEMORY.md");
    fs::create_dir_all(projection_path.parent().expect("projection directory"))
        .expect("create projection directory");
    fs::write(&projection_path, "stale forgotten memory\n").expect("write stale projection");

    let runtime =
        MemoryRuntime::open(memory_root, enabled_config()).expect("reopen memory runtime");
    drop(runtime);
    assert_eq!(
        fs::read_to_string(projection_path).expect("read rebuilt projection"),
        "# User Memory\n\n<!-- Generated from SQLite. Read-only; manual edits are not canonical. -->\n\n_No memory entries._\n"
    );
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-4
/// Verifies: startup replaces a stale projection for a scope retained only by scope state.
#[test]
fn startup_rebuilds_scope_state_only_project_projection() {
    let data_root = TempDir::new().expect("memory data root");
    let memory_root = data_root.path().join("memory");
    let runtime =
        MemoryRuntime::open(memory_root.clone(), enabled_config()).expect("create memory runtime");
    drop(runtime);

    let database_path = memory_root.join("memory.sqlite3");
    let connection = Connection::open(&database_path).expect("open memory database");
    connection
        .execute(
            "INSERT INTO memory_scope_state (
                 scope_type, scope_id, projection_revision, ignore_sources_before, last_rebuild_at
             ) VALUES ('project', 'project-scope', 1, '2026-01-02T00:00:00Z', NULL)",
            [],
        )
        .expect("insert scope-state-only project");
    drop(connection);

    let projection_path = memory_root
        .join("projects")
        .join("project-scope")
        .join("MEMORY.md");
    fs::create_dir_all(projection_path.parent().expect("projection directory"))
        .expect("create projection directory");
    fs::write(&projection_path, "stale project memory\n").expect("write stale projection");

    let runtime =
        MemoryRuntime::open(memory_root, enabled_config()).expect("reopen memory runtime");
    drop(runtime);
    assert_eq!(
        fs::read_to_string(projection_path).expect("read rebuilt projection"),
        "# Project Memory\n\n<!-- Generated from SQLite. Read-only; manual edits are not canonical. -->\n\n_No memory entries._\n"
    );
}
