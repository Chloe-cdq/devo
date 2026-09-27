use std::path::PathBuf;

use devo_core::MemoryConfig;
use devo_protocol::native::ids::MemoryEntryId;
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::{MemoryKind, MemoryScope, MemorySearchEntry, MemoryState};
use devo_server::memory::{MemoryCommand, MemoryCommandResult, MemoryRuntime, SearchMemoryRequest};
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
