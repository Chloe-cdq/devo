use super::*;
use crate::memory::{
    MemoryCommand, MemoryCommandResult, MemoryRememberRequest, MemorySourceContext,
};
use devo_protocol::native::rpc_memory::{MemoryKind, MemoryScope, MemoryStatus};
use devo_protocol::native::rpc_session::RelatedMemoryDeletion;
use pretty_assertions::assert_eq;

enum DeletionFailure {
    PreserveIntent,
    ForgetIntent,
    CanonicalCleanup,
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 Failure and Observability.
/// Verifies: failed preserve deletion intent is visible without a reconciliation or error job.
#[tokio::test]
async fn deletion_ledger_preserve_write_failure_is_visible() -> Result<()> {
    assert_deletion_failure_is_visible(DeletionFailure::PreserveIntent).await
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 Failure and Observability.
/// Verifies: failed forget deletion intent is visible without a reconciliation or error job.
#[tokio::test]
async fn deletion_ledger_forget_write_failure_is_visible() -> Result<()> {
    assert_deletion_failure_is_visible(DeletionFailure::ForgetIntent).await
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 Failure and Observability.
/// Verifies: canonical deletion worker storage failure is visible immediately and remains recorded after successful retry.
#[tokio::test]
async fn deletion_ledger_worker_cleanup_failure_is_visible() -> Result<()> {
    assert_deletion_failure_is_visible(DeletionFailure::CanonicalCleanup).await
}

async fn assert_deletion_failure_is_visible(failure: DeletionFailure) -> Result<()> {
    let (root, runtime, _) = setup(/*sources*/ 1, /*permits*/ 0)?;
    let source = runtime.deps.db.list_root_sessions()?[0].session_id;
    let memory = runtime.memory.as_ref().unwrap();
    memory
        .execute_command(MemoryCommand::Remember(MemoryRememberRequest {
            text: "Use tabs".into(),
            scope: MemoryScope::User,
            kind: Some(MemoryKind::Fact),
            source: MemorySourceContext {
                user_item_id: None,
                session_id: source,
                turn_id: None,
                workspace_root: root.path().into(),
            },
        }))
        .await?;
    let mut expected = MemoryStatus {
        enabled: true,
        storage_health: "healthy".into(),
        entry_count: 1,
        candidate_count: 0,
        pending_job_count: 0,
        retrying_job_count: 0,
        error_job_count: 0,
        last_successful_scan_at: None,
        rebuild: None,
        error_classes: vec![],
        source_exclusion_reasons: vec![],
    };
    assert_eq!(
        memory.execute_command(MemoryCommand::Status).await?,
        MemoryCommandResult::Status(expected.clone())
    );
    let ledger = rusqlite::Connection::open(root.path().join("devo.db"))?;
    let database = rusqlite::Connection::open(root.path().join("memory").join("memory.sqlite3"))?;
    match failure {
        DeletionFailure::PreserveIntent | DeletionFailure::ForgetIntent => {
            ledger.execute_batch(
                "CREATE TRIGGER reject_memory_intent BEFORE INSERT ON pending_memory_source_deletions
                 BEGIN SELECT RAISE(ABORT, 'private ledger diagnostic'); END;"
            )?;
            let related = match failure {
                DeletionFailure::PreserveIntent => RelatedMemoryDeletion::Preserve,
                DeletionFailure::ForgetIntent => RelatedMemoryDeletion::Forget,
                DeletionFailure::CanonicalCleanup => unreachable!(),
            };
            let error = runtime
                .delete_session_tree(source, related)
                .await
                .unwrap_err();
            assert!(error.starts_with("failed to record session deletion intent:"));
        }
        DeletionFailure::CanonicalCleanup => {
            database.execute_batch(
                "CREATE TRIGGER reject_memory_cleanup BEFORE DELETE ON memory_evidence
                 BEGIN SELECT RAISE(ABORT, 'private cleanup diagnostic'); END;",
            )?;
            let (reply, completion) = tokio::sync::oneshot::channel();
            memory.enqueue_source(crate::memory::scan::MemorySourceWork::DeleteSources {
                sources: vec![source],
                related_memory: RelatedMemoryDeletion::Preserve,
                reply,
            });
            assert!(matches!(
                completion.await?,
                Err(crate::memory::MemoryError::Database(_))
            ));
        }
    }
    // These failure paths have not queued reconciliation; status cannot be masked by a retry.
    assert!(runtime.deps.db.get_session(&source)?.is_some());
    expected.storage_health = "degraded".into();
    expected.error_classes = vec!["storage_error".into()];
    assert_eq!(
        memory.execute_command(MemoryCommand::Status).await?,
        MemoryCommandResult::Status(expected.clone())
    );
    match failure {
        DeletionFailure::PreserveIntent | DeletionFailure::ForgetIntent => {
            ledger.execute_batch("DROP TRIGGER reject_memory_intent")?;
            runtime
                .delete_session_tree(source, RelatedMemoryDeletion::Preserve)
                .await
                .map_err(anyhow::Error::msg)?;
        }
        DeletionFailure::CanonicalCleanup => {
            database.execute_batch("DROP TRIGGER reject_memory_cleanup")?;
            let (reply, completion) = tokio::sync::oneshot::channel();
            memory.enqueue_source(crate::memory::scan::MemorySourceWork::DeleteSources {
                sources: vec![source],
                related_memory: RelatedMemoryDeletion::Preserve,
                reply,
            });
            completion.await??;
        }
    }
    memory.reconcile_source_intents();
    assert_eq!(
        memory.execute_command(MemoryCommand::Status).await?,
        MemoryCommandResult::Status(expected)
    );
    Ok(())
}
