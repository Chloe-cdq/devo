use std::path::{Path, PathBuf};

use devo_core::MemoryConfig;
use devo_protocol::native::ids::MemoryEntryId;
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::{
    MemoryEntry, MemoryKind, MemoryOrigin, MemoryScope, MemorySearchEntry, MemoryState,
};
use devo_server::memory::{
    ListMemoryRequest, MemoryCommand, MemoryCommandResult, MemoryRuntime, SearchMemoryRequest,
};
use pretty_assertions::assert_eq;
use rusqlite::Connection;

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-10, DD-13
/// Verifies: the runtime search command owns default recall eligibility, ordering, and projection.
#[tokio::test]
async fn runtime_search_returns_bounded_active_and_restored_projections() {
    let data_root = tempfile::tempdir().expect("memory data root");
    let memory_root = data_root.path().join("memory");
    let runtime = MemoryRuntime::open(
        memory_root.clone(),
        MemoryConfig {
            enabled: true,
            ..MemoryConfig::default()
        },
    )
    .expect("create memory runtime");
    drop(runtime);

    let connection =
        Connection::open(memory_root.join("memory.sqlite3")).expect("open memory database");
    connection
        .execute_batch(
            "INSERT INTO memory_entries (
                 entry_id, scope_type, scope_id, kind, normalized_key, body,
                 origin, state, created_at, updated_at
             ) VALUES
                 ('active-entry', 'user', 'user', 'preference', 'editor active',
                  'Editor active', 'explicit_user', 'active',
                  '2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z'),
                 ('restored-entry', 'user', 'user', 'fact', 'editor restored',
                  'Editor restored', 'explicit_user', 'restored',
                  '2026-01-01T00:00:00Z', '2026-01-03T00:00:00Z'),
                 ('retired-entry', 'user', 'user', 'fact', 'editor retired',
                  'Editor retired', 'explicit_user', 'retired',
                  '2026-01-01T00:00:00Z', '2026-01-04T00:00:00Z');",
        )
        .expect("insert search fixtures");
    drop(connection);

    let runtime = MemoryRuntime::open(
        memory_root,
        MemoryConfig {
            enabled: true,
            ..MemoryConfig::default()
        },
    )
    .expect("reopen memory runtime");
    let result = match runtime
        .execute_command(MemoryCommand::Search(SearchMemoryRequest {
            query: "editor".to_owned(),
            scope: MemoryScope::User,
            kind: None,
            state: None,
            workspace_root: PathBuf::new(),
        }))
        .await
        .expect("search memory")
    {
        MemoryCommandResult::Search(result) => result,
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::PreparedForget(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::Read(_)
        | MemoryCommandResult::List(_) => panic!("expected search result"),
    };
    assert_eq!(
        result,
        Page {
            data: vec![
                MemorySearchEntry {
                    entry_id: MemoryEntryId::from_string("restored-entry".to_owned()),
                    scope: MemoryScope::User,
                    kind: MemoryKind::Fact,
                    state: MemoryState::Restored,
                    summary: "Editor restored".to_owned(),
                },
                MemorySearchEntry {
                    entry_id: MemoryEntryId::from_string("active-entry".to_owned()),
                    scope: MemoryScope::User,
                    kind: MemoryKind::Preference,
                    state: MemoryState::Active,
                    summary: "Editor active".to_owned(),
                },
            ],
            next_cursor: None,
        }
    );
}

const LITERAL_QUERY_CASES: &[(&str, &str, &str)] = &[
    ("API_KEY", "API_KEY", "APIXKEY"),
    ("100%", "100%", "1000"),
    ("a!_b%", "a!_b%", "a!Xb0"),
    ("api_key", "API_KEY", "APIXKEY"),
    ("EDITOR", "editor", "other"),
    (r"quoted'path\name", r"quoted'path\name", "quotedpath/name"),
];

fn literal_query_fixture(
    root: &Path,
    body: &str,
    normalized_key: &str,
    distractor: &str,
    distractor_count: usize,
) -> (MemoryRuntime, Vec<MemoryEntry>) {
    let memory_root = root.join("memory");
    let runtime = MemoryRuntime::open(
        memory_root.clone(),
        MemoryConfig {
            enabled: true,
            ..MemoryConfig::default()
        },
    )
    .expect("create memory runtime");
    let timestamp = "2026-01-01T00:00:00Z".parse().unwrap();
    let target = MemoryEntry {
        entry_id: MemoryEntryId::from_string("target".to_owned()),
        scope: MemoryScope::User,
        scope_id: "user".to_owned(),
        kind: MemoryKind::Fact,
        normalized_key: normalized_key.to_owned(),
        body: body.to_owned(),
        origin: MemoryOrigin::ExplicitUser,
        state: MemoryState::Active,
        created_at: timestamp,
        updated_at: timestamp,
        replacement_entry_id: None,
        provenance: vec![],
    };
    let mut entries = (0..distractor_count)
        .map(|index| MemoryEntry {
            entry_id: MemoryEntryId::from_string(format!("distractor-{index:02}")),
            normalized_key: format!("{distractor}-{index:02}"),
            body: distractor.to_owned(),
            ..target.clone()
        })
        .collect::<Vec<_>>();
    entries.push(target);
    let connection = Connection::open(memory_root.join("memory.sqlite3")).unwrap();
    for entry in &entries {
        connection
            .execute(
                "INSERT INTO memory_entries (
                     entry_id, scope_type, scope_id, kind, normalized_key, body,
                     origin, state, created_at, updated_at
                 ) VALUES (?1, 'user', 'user', 'fact', ?2, ?3,
                     'explicit_user', 'active', ?4, ?4)",
                rusqlite::params![
                    entry.entry_id.as_str(),
                    entry.normalized_key,
                    entry.body,
                    entry.created_at.to_rfc3339(),
                ],
            )
            .unwrap();
    }
    (runtime, entries)
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-10, DD-13
/// Verifies: literal query text cannot admit wildcard distractors that exhaust the search limit.
#[tokio::test]
async fn runtime_search_matches_literal_query_text() {
    for &(query, text, distractor) in LITERAL_QUERY_CASES {
        for (body, key) in [(text, "unrelated"), ("unrelated", text)] {
            let root = tempfile::tempdir().unwrap();
            let (runtime, entries) = literal_query_fixture(
                root.path(),
                body,
                key,
                distractor,
                /*distractor_count*/ 21,
            );
            let target = entries.last().unwrap();
            let result = runtime
                .execute_command(MemoryCommand::Search(SearchMemoryRequest {
                    query: query.to_owned(),
                    scope: MemoryScope::User,
                    kind: None,
                    state: None,
                    workspace_root: PathBuf::new(),
                }))
                .await
                .unwrap();
            assert_eq!(
                result,
                MemoryCommandResult::Search(Page {
                    data: vec![MemorySearchEntry {
                        entry_id: target.entry_id.clone(),
                        scope: MemoryScope::User,
                        kind: MemoryKind::Fact,
                        state: MemoryState::Active,
                        summary: body.to_owned(),
                    }],
                    next_cursor: None,
                }),
                "query {query:?} with body {body:?} and key {key:?}"
            );
        }
    }
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-10, DD-13
/// Verifies: list text filters match literal substrings without false matches or spurious pagination.
#[tokio::test]
async fn runtime_list_matches_literal_query_text() {
    for &(query, text, distractor) in LITERAL_QUERY_CASES {
        for (body, key) in [(text, "unrelated"), ("unrelated", text)] {
            let root = tempfile::tempdir().unwrap();
            let (runtime, entries) = literal_query_fixture(
                root.path(),
                body,
                key,
                distractor,
                /*distractor_count*/ 21,
            );
            let target = entries.last().unwrap().clone();
            let result = runtime
                .execute_command(MemoryCommand::List(ListMemoryRequest {
                    text: Some(query.to_owned()),
                    limit: Some(1),
                    ..ListMemoryRequest::default()
                }))
                .await
                .unwrap();
            assert_eq!(
                result,
                MemoryCommandResult::List(Page {
                    data: vec![target],
                    next_cursor: None,
                }),
                "query {query:?} with body {body:?} and key {key:?}"
            );
        }
    }

    let root = tempfile::tempdir().unwrap();
    let (runtime, entries) = literal_query_fixture(
        root.path(),
        "target body",
        "target key",
        "other",
        /*distractor_count*/ 1,
    );
    for text in [None, Some(String::new())] {
        let result = runtime
            .execute_command(MemoryCommand::List(ListMemoryRequest {
                text,
                ..ListMemoryRequest::default()
            }))
            .await
            .unwrap();
        assert_eq!(
            result,
            MemoryCommandResult::List(Page {
                data: entries.clone(),
                next_cursor: None,
            })
        );
    }
}
