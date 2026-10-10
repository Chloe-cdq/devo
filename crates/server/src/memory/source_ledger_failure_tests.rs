use super::*;
use devo_protocol::native::rpc_memory::{MemoryRecallEntry, MemoryStatus};
use pretty_assertions::assert_eq;

enum LedgerRead {
    Recall,
    SourceClaim,
    Remember,
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 Failure and Observability.
/// Verifies: foreground ledger read failure fences inference, preserves explicit recall and retains safe degraded status.
#[tokio::test]
async fn deletion_ledger_recall_read_failure_is_visible() {
    assert_ledger_read_failure_is_visible(LedgerRead::Recall).await;
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 Failure and Observability.
/// Verifies: source-specific ledger read failure prevents extraction claims and retains safe degraded status.
#[tokio::test]
async fn deletion_ledger_claim_read_failure_is_visible() {
    assert_ledger_read_failure_is_visible(LedgerRead::SourceClaim).await;
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 Failure and Observability.
/// Verifies: explicit write ledger read failure rejects the write and retains safe degraded status.
#[tokio::test]
async fn deletion_ledger_remember_read_failure_is_visible() {
    assert_ledger_read_failure_is_visible(LedgerRead::Remember).await;
}

async fn assert_ledger_read_failure_is_visible(read: LedgerRead) {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(crate::db::Database::open(root.path().join("devo.db")).unwrap());
    let mut runtime = open_runtime(&root.path().join("memory"));
    runtime.attach_deletion_ledger(db);
    let (source, candidate) = source();
    let now = chrono::DateTime::from_timestamp_millis(Utc::now().timestamp_millis()).unwrap();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &source, &[candidate], now)
        .unwrap();
    let inferred = runtime
        .list(ListMemoryRequest::default())
        .unwrap()
        .data
        .remove(0);
    let explicit = runtime
        .remember(remember_request("Keep tabs for scripts"))
        .unwrap();
    let mut expected = MemoryStatus {
        enabled: true,
        storage_health: "healthy".into(),
        entry_count: 2,
        candidate_count: 1,
        pending_job_count: 0,
        retrying_job_count: 0,
        error_job_count: 0,
        last_successful_scan_at: Some(now),
        rebuild: None,
        error_classes: vec![],
        source_exclusion_reasons: vec![],
    };
    assert_eq!(runtime.status().unwrap(), expected);
    let ledger = rusqlite::Connection::open(root.path().join("devo.db")).unwrap();
    ledger
        .execute_batch(
            "ALTER TABLE pending_memory_source_deletions RENAME TO unavailable_memory_ledger",
        )
        .unwrap();
    match read {
        LedgerRead::Recall => {
            let recalled = runtime
                .prepare_turn(PrepareMemoryRequest {
                    query: "tabs scripts".into(),
                    workspace_root: root.path().into(),
                    session_recall: MemorySetting::On,
                })
                .await
                .unwrap();
            assert_eq!(
                recalled.entries,
                vec![MemoryRecallEntry {
                    entry_id: explicit.entry_id.clone(),
                    scope: MemoryScope::User,
                    kind: MemoryKind::Preference,
                    summary: "Keep tabs for scripts".into(),
                    source_summary: "Explicit user memory".into(),
                }]
            );
        }
        LedgerRead::SourceClaim => {
            let mut newer = source.clone();
            newer.watermark = "source-2".into();
            // This checks source_has_intent without a global read or reconciliation.
            assert_eq!(runtime.claim_source(&newer, now).unwrap(), None);
        }
        LedgerRead::Remember => {
            assert!(matches!(
                runtime.remember(remember_request("Keep spaces for Python")),
                Err(super::super::MemoryError::InvalidRequest(_))
            ));
        }
    }
    expected.storage_health = "degraded".into();
    expected.error_classes = vec!["storage_error".into()];
    assert_eq!(runtime.status().unwrap(), expected);
    ledger
        .execute_batch(
            "ALTER TABLE unavailable_memory_ledger RENAME TO pending_memory_source_deletions",
        )
        .unwrap();
    let mut newer = source.clone();
    newer.watermark = "source-2".into();
    assert!(runtime.claim_source(&newer, now).unwrap().is_some());
    let recalled = runtime
        .prepare_turn(PrepareMemoryRequest {
            query: "tabs scripts".into(),
            workspace_root: root.path().into(),
            session_recall: MemorySetting::On,
        })
        .await
        .unwrap();
    assert_eq!(
        recalled.entries,
        vec![
            MemoryRecallEntry {
                entry_id: explicit.entry_id,
                scope: MemoryScope::User,
                kind: MemoryKind::Preference,
                summary: "Keep tabs for scripts".into(),
                source_summary: "Explicit user memory (1 source)".into(),
            },
            MemoryRecallEntry {
                entry_id: inferred.entry_id,
                scope: MemoryScope::User,
                kind: MemoryKind::Preference,
                summary: "I prefer tabs".into(),
                source_summary: "Inferred session memory (1 source)".into(),
            },
        ]
    );
    assert_eq!(runtime.status().unwrap(), expected);
}
