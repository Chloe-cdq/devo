use super::*;
use crate::memory::{
    MemoryCommand, MemoryCommandResult, MemoryRememberRequest, MemorySourceContext,
    PrepareMemoryRequest,
};
use devo_protocol::native::rpc_memory::{MemoryKind, MemoryRecallEntry, MemoryScope, MemoryStatus};
use devo_protocol::native::session::MemorySetting;
use pretty_assertions::assert_eq;

enum MaintenanceFailure {
    CandidateDeletion,
    ProjectionWrite,
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 Failure and Observability.
/// Verifies: failed retention is visible without an error job while foreground recall remains usable.
#[tokio::test]
async fn scan_reports_candidate_retention_failure_without_error_job() -> Result<()> {
    assert_maintenance_failure_is_visible(MaintenanceFailure::CandidateDeletion).await
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 Failure and Observability.
/// Verifies: failed maintenance projection is visible without an error job while foreground recall remains usable.
#[tokio::test]
async fn scan_reports_maintenance_projection_failure_without_error_job() -> Result<()> {
    assert_maintenance_failure_is_visible(MaintenanceFailure::ProjectionWrite).await
}

async fn assert_maintenance_failure_is_visible(failure: MaintenanceFailure) -> Result<()> {
    let (root, runtime, _) = setup(/*sources*/ 0, /*permits*/ 0)?;
    let memory = runtime.memory.as_ref().unwrap();
    memory
        .execute_command(MemoryCommand::Remember(MemoryRememberRequest {
            text: "Use tabs".into(),
            scope: MemoryScope::User,
            kind: None,
            source: MemorySourceContext {
                user_item_id: None,
                session_id: SessionId::new(),
                turn_id: None,
                workspace_root: root.path().into(),
            },
        }))
        .await?;
    let database = rusqlite::Connection::open(root.path().join("memory").join("memory.sqlite3"))?;
    database.execute_batch(
        "INSERT INTO memory_candidates (
            candidate_id, scope_type, scope_id, kind, normalized_key, body, origin,
            source_session_id, retention_until, created_at
        ) VALUES (
            'expired', 'user', 'user', 'fact', 'expired', 'private candidate', 'inferred',
            'source', '2000-01-01T00:00:00Z', '2000-01-01T00:00:00Z'
        );",
    )?;
    let connection = connect(&runtime).await?;
    let healthy = runtime
        .handle_incoming(
            connection,
            serde_json::json!({"id": 100, "method": "memory/status", "params": {}}),
        )
        .await
        .context("healthy memory status")?;
    let mut expected = MemoryStatus {
        enabled: true,
        storage_health: "healthy".into(),
        entry_count: 1,
        candidate_count: 1,
        pending_job_count: 0,
        retrying_job_count: 0,
        error_job_count: 0,
        last_successful_scan_at: None,
        rebuild: None,
        error_classes: vec![],
        source_exclusion_reasons: vec![],
    };
    assert_eq!(
        serde_json::from_value::<MemoryStatus>(healthy["result"].clone())?,
        expected
    );
    let before = memory
        .prepare_turn(PrepareMemoryRequest {
            query: "Use tabs".into(),
            workspace_root: root.path().into(),
            session_recall: MemorySetting::On,
        })
        .await?;
    assert!(!before.entries.is_empty());

    let projection = root.path().join("memory").join("user").join("MEMORY.md");
    match failure {
        MaintenanceFailure::CandidateDeletion => database.execute_batch(
            "CREATE TRIGGER reject_prune BEFORE DELETE ON memory_candidates
             BEGIN SELECT RAISE(ABORT, 'private database diagnostic'); END;",
        )?,
        MaintenanceFailure::ProjectionWrite => {
            std::fs::remove_file(&projection)?;
            std::fs::create_dir(&projection)?;
        }
    }
    assert!(scan(&runtime, root.path()).await.is_err());
    let after = memory
        .prepare_turn(PrepareMemoryRequest {
            query: "Use tabs".into(),
            workspace_root: root.path().into(),
            session_recall: MemorySetting::On,
        })
        .await?;
    assert_eq!(after, before);
    expected.storage_health = "degraded".into();
    expected.error_classes = vec!["storage_error".into()];
    expected.candidate_count = match failure {
        MaintenanceFailure::CandidateDeletion => 1,
        MaintenanceFailure::ProjectionWrite => 0,
    };
    let degraded = runtime
        .handle_incoming(
            connection,
            serde_json::json!({"id": 101, "method": "memory/status", "params": {}}),
        )
        .await
        .context("failed maintenance memory status")?;
    assert_eq!(
        serde_json::from_value::<MemoryStatus>(degraded["result"].clone())?,
        expected
    );

    match failure {
        MaintenanceFailure::CandidateDeletion => {
            database.execute_batch("DROP TRIGGER reject_prune")?
        }
        MaintenanceFailure::ProjectionWrite => std::fs::remove_dir(&projection)?,
    }
    scan(&runtime, root.path()).await?;
    expected.candidate_count = 0;
    let recovered = runtime
        .handle_incoming(
            connection,
            serde_json::json!({"id": 102, "method": "memory/status", "params": {}}),
        )
        .await
        .context("retained maintenance failure status")?;
    assert_eq!(
        serde_json::from_value::<MemoryStatus>(recovered["result"].clone())?,
        expected
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 Failure and Observability.
/// Verifies: deleted-source projection failure remains visible despite successful scanning and recall.
#[tokio::test]
async fn deletion_projection_failure_remains_visible_after_successful_scan() -> Result<()> {
    let (root, runtime, _) = setup(/*sources*/ 1, /*permits*/ 0)?;
    let source = runtime.deps.db.list_root_sessions()?[0].session_id;
    let memory = runtime.memory.as_ref().unwrap();
    let result = memory
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
    let MemoryCommandResult::Remember(entry) = result else {
        panic!("remember result");
    };
    let connection = connect(&runtime).await?;
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
    let healthy = runtime
        .handle_incoming(
            connection,
            serde_json::json!({"id": 100, "method": "memory/status", "params": {}}),
        )
        .await
        .context("healthy memory status")?;
    assert_eq!(
        serde_json::from_value::<MemoryStatus>(healthy["result"].clone())?,
        expected
    );

    let projection = root.path().join("memory").join("user").join("MEMORY.md");
    std::fs::remove_file(&projection)?;
    std::fs::create_dir(&projection)?;
    assert_eq!(
        runtime
            .delete_session_tree(
                source,
                devo_protocol::native::rpc_session::RelatedMemoryDeletion::Preserve,
            )
            .await
            .map_err(anyhow::Error::msg)?,
        vec![source]
    );
    memory.reconcile_source_intents();
    let database = rusqlite::Connection::open(root.path().join("memory").join("memory.sqlite3"))?;
    let pending: i64 = database.query_row(
        "SELECT COUNT(*) FROM memory_deleted_source_scopes",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(pending, 1);
    scan(&runtime, root.path()).await?;
    let recalled = memory
        .prepare_turn(PrepareMemoryRequest {
            query: "Use tabs".into(),
            workspace_root: root.path().into(),
            session_recall: MemorySetting::On,
        })
        .await?;
    assert_eq!(
        recalled.entries,
        vec![MemoryRecallEntry {
            entry_id: entry.entry_id,
            scope: MemoryScope::User,
            kind: MemoryKind::Fact,
            summary: "Use tabs".into(),
            source_summary: "Explicit user memory (0 sources)".into(),
        }]
    );
    expected.storage_health = "degraded".into();
    expected.error_classes = vec!["storage_error".into()];
    let degraded = runtime
        .handle_incoming(
            connection,
            serde_json::json!({"id": 101, "method": "memory/status", "params": {}}),
        )
        .await
        .context("deleted-source projection failure status")?;
    assert_eq!(
        serde_json::from_value::<MemoryStatus>(degraded["result"].clone())?,
        expected
    );

    std::fs::remove_dir(&projection)?;
    memory.reconcile_source_intents();
    let pending: i64 = database.query_row(
        "SELECT COUNT(*) FROM memory_deleted_source_scopes",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(pending, 0);
    let repaired = runtime
        .handle_incoming(
            connection,
            serde_json::json!({"id": 102, "method": "memory/status", "params": {}}),
        )
        .await
        .context("retained deletion projection failure status")?;
    assert_eq!(
        serde_json::from_value::<MemoryStatus>(repaired["result"].clone())?,
        expected
    );
    Ok(())
}
