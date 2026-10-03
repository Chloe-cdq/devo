use super::*;
use pretty_assertions::assert_eq;

/// Trace: L2-DES-MEM-001 Rev 4, L2-DES-SERVER-002.
/// Verifies: deleting a source finishes while memory storage is locked and leaves durable cleanup intent.
#[tokio::test]
async fn source_delete_does_not_wait_for_locked_memory_database() -> Result<()> {
    let (root, runtime, _) = setup(/*sources*/ 1, /*permits*/ 0)?;
    let source_id = runtime.deps.db.list_root_sessions()?[0].session_id;
    let blocker = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
    blocker.execute_batch("BEGIN IMMEDIATE")?;

    let deletion = tokio::time::timeout(
        Duration::from_secs(2),
        runtime.delete_session_tree(
            source_id,
            devo_protocol::native::rpc_session::RelatedMemoryDeletion::Preserve,
        ),
    )
    .await;
    let pending = runtime.deps.db.pending_memory_source_deletions()?;
    blocker.execute_batch("ROLLBACK")?;

    let deleted = deletion
        .context("session deletion waited for memory storage")?
        .map_err(anyhow::Error::msg)?;
    assert_eq!(deleted, vec![source_id]);
    assert_eq!(pending, vec![source_id]);
    assert!(runtime.deps.db.get_session(&source_id)?.is_none());
    Ok(())
}
