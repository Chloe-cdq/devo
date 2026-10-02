use super::*;
use crate::memory::{MemoryCommand, MemoryCommandResult};
use crate::support::initialize_connection;
use pretty_assertions::assert_eq;

enum SourceStoreFailure {
    RolloutMarker,
    IndexLedger,
    IndexLedgerAndProjection,
}

async fn hosted_web_survives_source_store_failure(failure: SourceStoreFailure) -> Result<()> {
    let root = TempDir::new()?;
    std::fs::write(
        root.path().join("config.toml"),
        "[memory]\nenabled = true\n[tools.web_search]\nmode = 'provider'\n",
    )?;
    let runtime = build_runtime_with_provider(
        root.path(),
        Arc::new(HostedWebProvider {
            calls: std::sync::atomic::AtomicUsize::new(0),
        }),
    );
    let (connection, mut notifications) = initialize_connection(&runtime).await?;
    let parent = start_durable_session(&runtime, connection, root.path()).await?;
    let record = runtime
        .session(parent)
        .await
        .context("parent")?
        .record()
        .await
        .flatten()
        .context("parent record")?;
    runtime
        .memory
        .as_ref()
        .context("memory")?
        .record_inferred(crate::memory::MemoryInferredRememberRequest {
            text: "I prefer tabs".into(),
            scope: devo_protocol::native::rpc_memory::MemoryScope::User,
            kind: None,
            source: crate::memory::MemorySourceContext {
                user_item_id: Some(devo_protocol::native::ids::ItemId::new()),
                session_id: parent,
                turn_id: Some(TurnId::new()),
                workspace_root: root.path().to_path_buf(),
            },
            source_observed_at: chrono::Utc::now() - chrono::Duration::hours(7),
            source_watermark: "before-external-use".into(),
        })?
        .context("existing inferred entry")?;
    match failure {
        SourceStoreFailure::RolloutMarker => {
            std::fs::remove_file(&record.rollout_path)?;
            std::fs::create_dir(&record.rollout_path)?;
        }
        SourceStoreFailure::IndexLedger | SourceStoreFailure::IndexLedgerAndProjection => {
            rusqlite::Connection::open(root.path().join("connection.db"))?.execute_batch(
                "CREATE TRIGGER reject_external_source BEFORE INSERT ON memory_external_context_sources
                 BEGIN SELECT RAISE(FAIL, 'injected source ledger failure'); END;",
            )?;
        }
    }
    let projection = root.path().join("memory/user/MEMORY.md");
    if matches!(failure, SourceStoreFailure::IndexLedgerAndProjection) {
        std::fs::remove_file(&projection)?;
        std::fs::create_dir(&projection)?;
    }
    super::lineage::spawn_and_complete(&runtime, &mut notifications, parent, "Search the web")
        .await?;
    let MemoryCommandResult::Status(status) = runtime
        .memory
        .as_ref()
        .context("memory")?
        .execute_command(MemoryCommand::Status)
        .await?
    else {
        panic!("expected memory status");
    };
    assert_eq!(status.storage_health, "degraded");
    assert!(
        status
            .error_classes
            .iter()
            .any(|class| class == "source_provenance_storage")
    );
    runtime.shutdown().await;
    drop(runtime);

    // Inspect durable exclusion after the original runtime and its actors stop.
    // A projection marker failure must not erase the authoritative ledger;
    // a ledger failure must leave the memory database's source exclusion intact.
    match failure {
        SourceStoreFailure::RolloutMarker => {
            let db = crate::db::Database::open(root.path().join("connection.db"))?;
            assert!(db.has_external_context_source(&parent.to_string())?);
        }
        SourceStoreFailure::IndexLedger | SourceStoreFailure::IndexLedgerAndProjection => {
            let memory = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
            assert!(memory.query_row(
                "SELECT EXISTS(SELECT 1 FROM memory_excluded_sources WHERE source_session_id = ?1)",
                [parent.to_string()],
                |row| row.get::<_, bool>(0),
            )?);
            assert_eq!(external_context_fact_count(&record.rollout_path)?, 1);
        }
    }
    if matches!(failure, SourceStoreFailure::IndexLedgerAndProjection) {
        std::fs::remove_dir(&projection)?;
    }
    let restarted = build_runtime(root.path());
    let MemoryCommandResult::Search(result) = restarted
        .memory
        .as_ref()
        .context("restarted memory")?
        .execute_command(MemoryCommand::Search(crate::memory::SearchMemoryRequest {
            query: "tabs".into(),
            scope: devo_protocol::native::rpc_memory::MemoryScope::User,
            kind: None,
            state: None,
            workspace_root: root.path().to_path_buf(),
        }))
        .await?
    else {
        panic!("expected memory search result");
    };
    assert!(result.data.is_empty());
    restarted.shutdown().await;
    Ok(())
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: a rollout marker failure preserves foreground Web and restart-safe source exclusion.
#[tokio::test]
async fn hosted_web_survives_rollout_marker_failure() -> Result<()> {
    hosted_web_survives_source_store_failure(SourceStoreFailure::RolloutMarker).await
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: a source-ledger write failure falls back to durable memory exclusion without failing Web.
#[tokio::test]
async fn hosted_web_survives_index_ledger_failure() -> Result<()> {
    hosted_web_survives_source_store_failure(SourceStoreFailure::IndexLedger).await
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: committed fallback exclusion survives projection failure and keeps old inferred content out of search after restart.
#[tokio::test]
async fn hosted_web_survives_committed_exclusion_projection_failure() -> Result<()> {
    hosted_web_survives_source_store_failure(SourceStoreFailure::IndexLedgerAndProjection).await
}
