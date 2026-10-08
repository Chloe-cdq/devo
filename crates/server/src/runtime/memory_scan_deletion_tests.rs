use super::*;
use crate::memory::{
    ListMemoryRequest, MemoryCommand, MemoryCommandResult, MemoryRememberRequest,
    MemorySourceContext,
};
use devo_protocol::native::rpc_memory::{MemoryKind, MemoryScope, MemoryState};
use pretty_assertions::assert_eq;

async fn remember(
    runtime: &ServerRuntime,
    session_id: SessionId,
    root: &std::path::Path,
    text: &str,
) -> Result<devo_protocol::native::rpc_memory::MemoryEntry> {
    let result = runtime
        .memory
        .as_ref()
        .unwrap()
        .execute_command(MemoryCommand::Remember(MemoryRememberRequest {
            text: text.into(),
            scope: MemoryScope::User,
            kind: Some(MemoryKind::Preference),
            source: MemorySourceContext {
                user_item_id: None,
                session_id,
                turn_id: None,
                workspace_root: root.into(),
            },
        }))
        .await?;
    let MemoryCommandResult::Remember(entry) = result else {
        panic!("remember result")
    };
    Ok(entry)
}

/// Trace: L2-DES-MEM-001 Entry Lifecycle and Retention.
/// Verifies: Native related-memory deletion revokes related identities, including ones with other evidence.
#[tokio::test]
async fn native_related_memory_deletion_revokes_shared_explicit_entries() -> Result<()> {
    let (root, runtime, _) = setup(/*sources*/ 2, /*permits*/ 0)?;
    let sources = runtime.deps.db.list_root_sessions()?;
    let source_id = sources[0].session_id;
    let other_id = sources[1].session_id;
    let entry = remember(&runtime, source_id, root.path(), "I prefer tabs").await?;
    remember(&runtime, other_id, root.path(), "I prefer tabs").await?;
    let unrelated = remember(&runtime, other_id, root.path(), "I prefer Rust").await?;
    let connection_id = connect(&runtime).await?;
    let response = runtime.handle_incoming(connection_id, serde_json::json!({
        "id": 13, "method": "session/delete", "params": {"sessionId": source_id, "relatedMemory": "forget"}
    })).await.context("delete response")?;
    assert_eq!(response.get("error"), None);
    runtime.memory.as_ref().unwrap().reconcile_source_intents();
    let connection = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
    let stored: (String, i64, i64) = connection.query_row(
        "SELECT state, (SELECT COUNT(*) FROM memory_revocations WHERE normalized_key = entry.normalized_key AND restored_at IS NULL), (SELECT COUNT(*) FROM memory_entries_fts WHERE entry_id = entry.entry_id) FROM memory_entries entry WHERE entry_id = ?1",
        [entry.entry_id.as_str()], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
    assert_eq!(stored, ("retired".into(), 1, 0));
    assert!(runtime.deps.db.get_session(&other_id)?.is_some());
    let result = runtime
        .memory
        .as_ref()
        .unwrap()
        .execute_command(MemoryCommand::List(ListMemoryRequest {
            state: Some(MemoryState::Active),
            ..ListMemoryRequest::default()
        }))
        .await?;
    let MemoryCommandResult::List(page) = result else {
        panic!("list result")
    };
    assert_eq!(page.data, vec![unrelated]);
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-6.
/// Verifies: related-memory deletion observes the same global deletion lease as memory/forget.
#[tokio::test]
async fn related_memory_delete_rejects_an_active_forget_lease() -> Result<()> {
    let (root, runtime, _) = setup(/*sources*/ 1, /*permits*/ 0)?;
    let source_id = runtime.deps.db.list_root_sessions()?[0].session_id;
    let entry = remember(&runtime, source_id, root.path(), "I prefer tabs").await?;
    let connection_id = connect(&runtime).await?;
    let _reservation = runtime
        .memory_forget_coordinator
        .authorize_native(source_id, Some(&entry.entry_id))?;
    let response = runtime.handle_incoming(connection_id, serde_json::json!({
        "id": 13, "method": "session/delete", "params": {"sessionId": source_id, "relatedMemory": "forget"}
    })).await.context("delete response")?;
    assert!(
        response.get("error").is_some(),
        "related-memory deletion must reject an active forget lease: {response}"
    );
    assert!(runtime.deps.db.get_session(&source_id)?.is_some());
    Ok(())
}

/// Trace: L2-DES-MEM-001 Entry Lifecycle and Retention.
/// Verifies: ordinary deletion preserves explicit memory and removes only deleted-session provenance.
#[tokio::test]
async fn ordinary_delete_preserves_explicit_memory() -> Result<()> {
    let (root, runtime, _) = setup(/*sources*/ 1, /*permits*/ 0)?;
    let source_id = runtime.deps.db.list_root_sessions()?[0].session_id;
    let mut expected = remember(&runtime, source_id, root.path(), "I prefer tabs").await?;
    let connection_id = connect(&runtime).await?;
    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 13, "method": "session/delete", "params": {"sessionId": source_id}
            }),
        )
        .await
        .context("delete response")?;
    assert_eq!(response.get("error"), None);
    runtime.memory.as_ref().unwrap().reconcile_source_intents();
    expected.provenance.clear();
    let result = runtime
        .memory
        .as_ref()
        .unwrap()
        .execute_command(MemoryCommand::List(ListMemoryRequest::default()))
        .await?;
    let MemoryCommandResult::List(page) = result else {
        panic!("list result")
    };
    assert_eq!(page.data, vec![expected]);
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-6 and Entry Lifecycle and Retention.
/// Verifies: a canonical cleanup failure rolls back revocation and keeps the source session retryable.
#[tokio::test]
async fn related_memory_delete_rolls_back_when_evidence_cleanup_fails() -> Result<()> {
    let (root, runtime, _) = setup(/*sources*/ 1, /*permits*/ 0)?;
    let source_id = runtime.deps.db.list_root_sessions()?[0].session_id;
    let entry = remember(&runtime, source_id, root.path(), "I prefer tabs").await?;
    let connection = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
    connection.execute_batch("CREATE TRIGGER fail_evidence_delete BEFORE DELETE ON memory_evidence BEGIN SELECT RAISE(ABORT, 'cleanup failed'); END;")?;
    let connection_id = connect(&runtime).await?;
    let response = runtime.handle_incoming(connection_id, serde_json::json!({
        "id": 13, "method": "session/delete", "params": {"sessionId": source_id, "relatedMemory": "forget"}
    })).await.context("delete response")?;
    assert!(
        response.get("error").is_some(),
        "canonical cleanup must fail atomically: {response}"
    );
    assert!(runtime.deps.db.get_session(&source_id)?.is_some());
    let stored: (String, i64, i64, i64) = connection.query_row(
        "SELECT state, (SELECT COUNT(*) FROM memory_revocations), (SELECT COUNT(*) FROM memory_deleted_sources), (SELECT COUNT(*) FROM memory_evidence) FROM memory_entries WHERE entry_id = ?1",
        [entry.entry_id.as_str()], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))?;
    assert_eq!(stored, ("active".into(), 0, 0, 1));
    connection.execute_batch("DROP TRIGGER fail_evidence_delete;")?;
    Ok(())
}

/// Trace: L2-DES-MEM-001 Entry Lifecycle and Retention.
/// Verifies: related deletion succeeds after canonical revocation even when its projection cannot refresh.
#[tokio::test]
async fn related_memory_delete_succeeds_despite_projection_failure() -> Result<()> {
    let (root, runtime, _) = setup(/*sources*/ 1, /*permits*/ 0)?;
    let source_id = runtime.deps.db.list_root_sessions()?[0].session_id;
    let entry = remember(&runtime, source_id, root.path(), "I prefer tabs").await?;
    let projection = root.path().join("memory/user");
    std::fs::remove_file(projection.join("MEMORY.md"))?;
    std::fs::remove_dir(&projection)?;
    std::fs::write(&projection, "blocked")?;
    let connection_id = connect(&runtime).await?;
    let response = runtime.handle_incoming(connection_id, serde_json::json!({
        "id": 13, "method": "session/delete", "params": {"sessionId": source_id, "relatedMemory": "forget"}
    })).await.context("delete response")?;
    assert_eq!(response.get("error"), None);
    assert!(runtime.deps.db.get_session(&source_id)?.is_none());
    let connection = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
    let stored: (String, i64, i64) = connection.query_row(
        "SELECT state, (SELECT COUNT(*) FROM memory_revocations WHERE normalized_key = entry.normalized_key AND restored_at IS NULL), (SELECT COUNT(*) FROM memory_evidence WHERE session_id = ?2) FROM memory_entries entry WHERE entry_id = ?1",
        rusqlite::params![entry.entry_id.as_str(), source_id.to_string()], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
    assert_eq!(stored, ("retired".into(), 1, 0));
    std::fs::remove_file(&projection)?;
    runtime.memory.as_ref().unwrap().reconcile_source_intents();
    let repaired = std::fs::read_to_string(projection.join("MEMORY.md"))?;
    assert!(repaired.contains("state: retired"));
    assert!(!repaired.contains(&source_id.to_string()));
    Ok(())
}

/// Trace: L2-DES-MEM-001 Entry Lifecycle and Retention.
/// Verifies: a failed projection does not hide inferred memory whose other evidence survives canonical cleanup.
#[tokio::test]
async fn surviving_evidence_remains_available_during_projection_repair() -> Result<()> {
    let (root, runtime, _) = setup(/*sources*/ 2, /*permits*/ 2)?;
    let sources = runtime.deps.db.list_root_sessions()?;
    let source_id = sources[0].session_id;
    scan(&runtime, root.path()).await?;
    let memory = runtime.memory.as_ref().unwrap();
    let before = memory
        .execute_command(MemoryCommand::List(ListMemoryRequest::default()))
        .await?;
    let MemoryCommandResult::List(before) = before else {
        panic!("list result")
    };
    assert_eq!(before.data.len(), 1);
    let mut expected = before.data[0].clone();
    expected.provenance.retain(|evidence| {
        evidence.source_session_id.as_deref() != Some(source_id.to_string().as_str())
    });
    let projection = root.path().join("memory/user");
    std::fs::remove_file(projection.join("MEMORY.md"))?;
    std::fs::remove_dir(&projection)?;
    std::fs::write(&projection, "blocked")?;
    let connection_id = connect(&runtime).await?;
    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 13, "method": "session/delete", "params": {"sessionId": source_id}
            }),
        )
        .await
        .context("delete response")?;
    assert_eq!(response.get("error"), None);
    memory.reconcile_source_intents();
    let after = memory
        .execute_command(MemoryCommand::List(ListMemoryRequest::default()))
        .await?;
    let MemoryCommandResult::List(after) = after else {
        panic!("list result")
    };
    assert_eq!(after.data, vec![expected]);
    assert_eq!(runtime.deps.db.pending_memory_source_deletions()?, vec![]);
    std::fs::remove_file(&projection)?;
    memory.reconcile_source_intents();
    assert!(projection.join("MEMORY.md").is_file());
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-6.
/// Verifies: bulk related revocation invalidates old search snapshots and preserves unrelated pending candidates.
#[tokio::test]
async fn related_deletion_invalidates_search_without_losing_unrelated_candidates() -> Result<()> {
    let (root, runtime, _) = setup(/*sources*/ 2, /*permits*/ 0)?;
    let sources = runtime.deps.db.list_root_sessions()?;
    let source_id = sources[0].session_id;
    let other_id = sources[1].session_id;
    let related = remember(&runtime, source_id, root.path(), "I prefer tabs").await?;
    let unrelated = remember(&runtime, other_id, root.path(), "I prefer Rust").await?;
    let invocation = devo_core::tools::MemoryToolInvocation {
        session_id: other_id,
        turn_id: devo_protocol::TurnId::new(),
        user_item_id: devo_protocol::native::ids::ItemId::new(),
    };
    let candidates = [&related, &unrelated]
        .into_iter()
        .map(
            |entry| devo_protocol::native::rpc_memory::MemorySearchEntry {
                entry_id: entry.entry_id.clone(),
                scope: entry.scope,
                kind: entry.kind,
                state: entry.state,
                summary: entry.body.clone(),
            },
        )
        .collect::<Vec<_>>();
    let coordinator = &runtime.memory_forget_coordinator;
    let epoch = coordinator.begin_search()?;
    coordinator.record_search_snapshot(&invocation, &candidates, epoch)?;
    let connection_id = connect(&runtime).await?;
    let response = runtime.handle_incoming(connection_id, serde_json::json!({
        "id": 13, "method": "session/delete", "params": {"sessionId": source_id, "relatedMemory": "forget"}
    })).await.context("delete response")?;
    assert_eq!(response.get("error"), None);
    assert!(
        coordinator
            .record_search_snapshot(&invocation, &candidates, epoch)
            .is_err()
    );
    let selection = devo_core::tools::MemoryToolInvocation {
        turn_id: devo_protocol::TurnId::new(),
        user_item_id: devo_protocol::native::ids::ItemId::new(),
        ..invocation
    };
    assert!(
        coordinator
            .authorize_agent(
                &selection,
                "Forget the related preference",
                &related.entry_id,
                MemoryScope::User
            )
            .is_err()
    );
    let reservation = coordinator.authorize_agent(
        &selection,
        "Forget the Rust preference",
        &unrelated.entry_id,
        MemoryScope::User,
    )?;
    drop(reservation);
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-6, Entry Lifecycle and Retention.
/// Verifies: a failed ordinary deletion remains retryable with related revocation after restart.
#[tokio::test]
async fn related_memory_retry_after_rollout_failure_revokes_explicit_memory() -> Result<()> {
    let (root, runtime, provider) = setup(/*sources*/ 2, /*permits*/ 0)?;
    let sources = runtime.deps.db.list_root_sessions()?;
    let source_id = sources[0].session_id;
    let other_id = sources[1].session_id;
    let entry = remember(&runtime, source_id, root.path(), "I prefer tabs").await?;
    let unrelated = remember(&runtime, other_id, root.path(), "I prefer Rust").await?;
    let rollout = root
        .path()
        .join("sessions")
        .join(format!("source-{source_id}.jsonl"));
    let journal = std::fs::read(&rollout)?;
    std::fs::write(&rollout, "{\"unknown_record\":{}}\n")?;
    let connection_id = connect(&runtime).await?;
    let failed = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 13, "method": "session/delete", "params": {"sessionId": source_id}
            }),
        )
        .await
        .context("failed delete response")?;
    assert!(
        failed.get("error").is_some(),
        "rollout deletion must fail: {failed}"
    );
    assert!(runtime.deps.db.get_session(&source_id)?.is_some());
    let connection = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
    let preserved: (String, i64, i64, i64, i64) = connection.query_row(
        "SELECT state, (SELECT COUNT(*) FROM memory_revocations WHERE normalized_key = entry.normalized_key), (SELECT COUNT(*) FROM memory_evidence WHERE session_id = ?2), (SELECT COUNT(*) FROM memory_entries_fts WHERE entry_id = entry.entry_id), (SELECT COUNT(*) FROM memory_deleted_source_entries WHERE source_session_id = ?2) FROM memory_entries entry WHERE entry_id = ?1",
        rusqlite::params![entry.entry_id.as_str(), source_id.to_string()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
    )?;
    assert_eq!(preserved, ("active".into(), 0, 0, 1, 1));
    drop(connection);
    std::fs::write(&rollout, journal)?;
    drop(runtime);
    let runtime = open_scan_runtime(root.path(), provider)?;
    runtime.memory.as_ref().unwrap().reconcile_source_intents();
    let connection_id = connect(&runtime).await?;
    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 14, "method": "session/delete", "params": {
                    "sessionId": source_id, "relatedMemory": "forget"
                }
            }),
        )
        .await
        .context("retry delete response")?;
    assert_eq!(response.get("error"), None);
    assert!(runtime.deps.db.get_session(&source_id)?.is_none());
    runtime.memory.as_ref().unwrap().reconcile_source_intents();
    let connection = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
    let forgotten: (String, i64, i64, i64, i64) = connection.query_row(
        "SELECT state, (SELECT COUNT(*) FROM memory_revocations WHERE normalized_key = entry.normalized_key AND restored_at IS NULL), (SELECT COUNT(*) FROM memory_evidence WHERE session_id = ?2), (SELECT COUNT(*) FROM memory_entries_fts WHERE entry_id = entry.entry_id), (SELECT COUNT(*) FROM memory_deleted_source_entries WHERE source_session_id = ?2) FROM memory_entries entry WHERE entry_id = ?1",
        rusqlite::params![entry.entry_id.as_str(), source_id.to_string()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
    )?;
    assert_eq!(forgotten, ("retired".into(), 1, 0, 0, 0));
    let active = runtime
        .memory
        .as_ref()
        .unwrap()
        .execute_command(MemoryCommand::List(ListMemoryRequest {
            state: Some(MemoryState::Active),
            ..ListMemoryRequest::default()
        }))
        .await?;
    assert_eq!(
        active,
        MemoryCommandResult::List(devo_protocol::native::page::Page {
            data: vec![unrelated],
            next_cursor: None,
        })
    );
    Ok(())
}
