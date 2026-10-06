use std::sync::Arc;

use devo_protocol::native::rpc_session::RelatedMemoryDeletion;
use pretty_assertions::assert_eq;

use super::runtime_test_support::{open_runtime, remember_request};
use super::{MemoryCommand, MemoryError, scan::MemorySourceWork};

/// Trace: L2-DES-MEM-001 Entry Lifecycle and Retention.
/// Verifies: canonical Interface cleanup completes before projection repair is scheduled.
#[tokio::test]
async fn canonical_cleanup_does_not_refresh_projections() {
    let root = tempfile::tempdir().unwrap();
    let runtime = Arc::new(open_runtime(root.path()));
    let request = remember_request("I prefer tabs");
    let source = request.source.session_id;
    runtime
        .execute_command(MemoryCommand::Remember(request))
        .await
        .unwrap();
    let projection = root.path().join("user/MEMORY.md");
    let before = std::fs::read_to_string(&projection).unwrap();
    let (reply, completion) = tokio::sync::oneshot::channel();
    runtime.enqueue_source(MemorySourceWork::DeleteSources {
        sources: vec![source],
        related_memory: RelatedMemoryDeletion::Forget,
        reply,
    });
    assert_eq!(completion.await.unwrap().unwrap().len(), 1);
    assert_eq!(std::fs::read_to_string(&projection).unwrap(), before);
    let connection = runtime.connection.lock().unwrap();
    let stored: (String, i64) = connection
        .query_row(
            "SELECT state, (SELECT COUNT(*) FROM memory_deleted_source_scopes) FROM memory_entries",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(stored, ("retired".into(), 1));
    drop(connection);
    runtime.reconcile_source_intents();
    assert!(
        std::fs::read_to_string(projection)
            .unwrap()
            .contains("state: retired")
    );
}

/// Trace: L2-DES-MEM-001 Entry Lifecycle and Retention.
/// Verifies: a stale explicit write cannot restore revocations or recreate deleted-source evidence.
#[tokio::test]
async fn deleted_source_rejects_explicit_remember() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let request = remember_request("I prefer tabs");
    runtime
        .execute_command(MemoryCommand::Remember(request.clone()))
        .await
        .unwrap();
    runtime
        .delete_sources(
            &[request.source.session_id],
            chrono::Utc::now(),
            RelatedMemoryDeletion::Forget,
        )
        .unwrap();
    assert!(matches!(
        runtime
            .execute_command(MemoryCommand::Remember(request))
            .await,
        Err(MemoryError::InvalidRequest(_))
    ));
    let connection = runtime.connection.lock().unwrap();
    let stored: (String, i64, i64) = connection.query_row(
        "SELECT state, (SELECT COUNT(*) FROM memory_revocations WHERE restored_at IS NULL), (SELECT COUNT(*) FROM memory_evidence) FROM memory_entries",
        [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).unwrap();
    assert_eq!(stored, ("retired".into(), 1, 0));
}

/// Trace: L2-DES-MEM-001 Entry Lifecycle and Retention.
/// Verifies: deferred cleanup intent rejects explicit writes before a tombstone can be committed.
#[tokio::test]
async fn pending_source_deletion_rejects_explicit_remember() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(crate::db::Database::open(root.path().join("devo.db")).unwrap());
    let mut runtime = open_runtime(&root.path().join("memory"));
    runtime.attach_deletion_ledger(Arc::clone(&db));
    let request = remember_request("I prefer tabs");
    db.record_memory_source_deletions(&[request.source.session_id])
        .unwrap();
    assert!(matches!(
        runtime
            .execute_command(MemoryCommand::Remember(request))
            .await,
        Err(MemoryError::InvalidRequest(_))
    ));
    let connection = runtime.connection.lock().unwrap();
    let counts: (i64, i64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM memory_entries), (SELECT COUNT(*) FROM memory_evidence)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(counts, (0, 0));
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-6, Entry Lifecycle and Retention.
/// Verifies: merging a legacy identity preserves failed-deletion associations for normal revocation.
#[tokio::test]
async fn related_memory_retry_preserves_merged_legacy_identity() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let request = remember_request("FOO=1");
    runtime
        .execute_command(MemoryCommand::Remember(request.clone()))
        .await
        .unwrap();
    let source = devo_protocol::SessionId::new();
    runtime.connection.lock().unwrap().execute(
        "INSERT INTO memory_entries(entry_id, scope_type, scope_id, kind, normalized_key, body, origin, state, created_at, updated_at)
         VALUES('legacy-assignment', 'user', 'user', 'preference', 'foo1', 'FOO=1', 'inferred_session', 'active', '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z')",
        [],
    ).unwrap();
    runtime.connection.lock().unwrap().execute(
        "INSERT INTO memory_evidence VALUES('legacy-evidence', 'legacy-assignment', ?1, NULL, NULL, '2026-09-01T00:00:00Z', 'legacy-watermark')",
        [source.to_string()],
    ).unwrap();
    runtime
        .delete_sources(
            &[source],
            chrono::Utc::now(),
            RelatedMemoryDeletion::Preserve,
        )
        .unwrap();
    runtime
        .execute_command(MemoryCommand::Remember(request))
        .await
        .unwrap();
    runtime
        .delete_sources(&[source], chrono::Utc::now(), RelatedMemoryDeletion::Forget)
        .unwrap();
    let connection = runtime.connection.lock().unwrap();
    let stored: (String, i64, i64, i64) = connection.query_row(
        "SELECT state,
           (SELECT COUNT(*) FROM memory_revocations WHERE normalized_key = entry.normalized_key AND restored_at IS NULL),
           (SELECT COUNT(*) FROM memory_entries_fts WHERE entry_id = entry.entry_id),
           (SELECT COUNT(*) FROM memory_deleted_source_entries WHERE source_session_id = ?1 AND entry_id = entry.entry_id)
         FROM memory_entries entry",
        [source.to_string()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    ).unwrap();
    assert_eq!(stored, ("retired".into(), 1, 0, 1));
}
