use super::*;
use crate::memory::{MemoryCommand, MemoryCommandResult};
use crate::support::initialize_connection;
use pretty_assertions::assert_eq;

enum SourceStoreFailure {
    RolloutMarker,
    IndexLedger,
    IndexLedgerAndProjection,
    BothDatabases,
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
        SourceStoreFailure::IndexLedger
        | SourceStoreFailure::IndexLedgerAndProjection
        | SourceStoreFailure::BothDatabases => {
            rusqlite::Connection::open(root.path().join("connection.db"))?.execute_batch(
                "CREATE TRIGGER reject_external_source BEFORE INSERT ON memory_external_context_sources
                 BEGIN SELECT RAISE(FAIL, 'injected source ledger failure'); END;",
            )?;
        }
    }
    if matches!(failure, SourceStoreFailure::BothDatabases) {
        rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?.execute_batch(
            "CREATE TRIGGER reject_external_exclusion BEFORE INSERT ON memory_excluded_sources
             BEGIN SELECT RAISE(FAIL, 'injected exclusion failure'); END;",
        )?;
    }
    let projection = root.path().join("memory/user/MEMORY.md");
    if matches!(failure, SourceStoreFailure::IndexLedgerAndProjection) {
        std::fs::remove_file(&projection)?;
        std::fs::create_dir(&projection)?;
    }
    super::lineage::spawn_and_complete(&runtime, &mut notifications, parent, "Search the web")
        .await?;
    runtime
        .memory
        .as_ref()
        .context("memory")?
        .reconcile_source_intents();
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
        SourceStoreFailure::BothDatabases => {
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
    if matches!(failure, SourceStoreFailure::BothDatabases) {
        rusqlite::Connection::open(root.path().join("connection.db"))?
            .execute_batch("DROP TRIGGER reject_external_source;")?;
        rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?
            .execute_batch("DROP TRIGGER reject_external_exclusion;")?;
        let memory = restarted.memory.as_ref().context("memory")?;
        memory.reconcile_source_intents();
        let connection = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
        let state: String = connection.query_row(
            "SELECT state FROM memory_entries WHERE body = 'I prefer tabs'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(state, "retired");
    }
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

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: both exclusion databases can fail without blocking hosted Web; canonical history fences inferred memory across restart and repair.
#[tokio::test]
async fn hosted_web_survives_both_exclusion_databases_failing() -> Result<()> {
    hosted_web_survives_source_store_failure(SourceStoreFailure::BothDatabases).await
}

enum MemoryStore {
    Disabled,
    Unavailable,
}

async fn hosted_web_without_memory_store(memory_store: MemoryStore) -> Result<()> {
    let root = TempDir::new()?;
    if matches!(memory_store, MemoryStore::Unavailable) {
        std::fs::write(root.path().join("memory"), "unavailable")?;
    }
    std::fs::write(
        root.path().join("config.toml"),
        "[tools.web_search]\nmode = 'provider'\n",
    )?;
    let runtime = build_runtime_with_provider(
        root.path(),
        Arc::new(HostedWebProvider {
            calls: std::sync::atomic::AtomicUsize::new(0),
        }),
    );
    let connection = initialized_connection(&runtime).await;
    let session = start_durable_session(&runtime, connection, root.path()).await?;
    let record = runtime
        .session(session)
        .await
        .context("session")?
        .record()
        .await
        .flatten()
        .context("record")?;
    rusqlite::Connection::open(root.path().join("connection.db"))?.execute_batch(
        "CREATE TRIGGER reject_external_source BEFORE INSERT ON memory_external_context_sources
         BEGIN SELECT RAISE(FAIL, 'injected source ledger failure'); END;",
    )?;
    if runtime.memory.is_some() {
        rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?.execute_batch(
            "CREATE TRIGGER reject_external_exclusion BEFORE INSERT ON memory_excluded_sources
             BEGIN SELECT RAISE(FAIL, 'injected exclusion failure'); END;",
        )?;
    }
    let turn = start_turn(&runtime, connection, session, "Search the web").await?;
    assert_eq!(
        completed_turn(&runtime, turn).await?.status,
        TurnStatus::Completed
    );
    assert_eq!(external_context_fact_count(&record.rollout_path)?, 1);
    runtime.shutdown().await;
    Ok(())
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: globally disabled memory cannot make database failures block a durable foreground Web turn.
#[tokio::test]
async fn disabled_memory_database_failures_do_not_block_hosted_web() -> Result<()> {
    hosted_web_without_memory_store(MemoryStore::Disabled).await
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: failure to initialize optional memory plus a failed source-ledger write still permits Web and records canonical provenance.
#[tokio::test]
async fn unavailable_memory_and_failed_ledger_do_not_block_hosted_web() -> Result<()> {
    hosted_web_without_memory_store(MemoryStore::Unavailable).await
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: local Tool Search dispatch succeeds when both memory exclusion stores reject writes.
#[tokio::test]
async fn tool_search_survives_both_exclusion_databases_failing() -> Result<()> {
    let root = TempDir::new()?;
    std::fs::write(
        root.path().join("config.toml"),
        "[memory]\nenabled = true\n[tools.web_search]\nmode = 'disabled'\n[tools.web_fetch]\nmode = 'disabled'\n",
    )?;
    let runtime = build_runtime_with_default_tools(
        root.path(),
        Arc::new(ToolSearchProvider {
            calls: std::sync::atomic::AtomicUsize::new(0),
        }),
    );
    let connection = initialized_connection(&runtime).await;
    let session = start_durable_session(&runtime, connection, root.path()).await?;
    let record = runtime
        .session(session)
        .await
        .context("session")?
        .record()
        .await
        .flatten()
        .context("record")?;
    rusqlite::Connection::open(root.path().join("external_context.db"))?.execute_batch(
        "CREATE TRIGGER reject_external_source BEFORE INSERT ON memory_external_context_sources
         BEGIN SELECT RAISE(FAIL, 'injected source ledger failure'); END;",
    )?;
    rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?.execute_batch(
        "CREATE TRIGGER reject_external_exclusion BEFORE INSERT ON memory_excluded_sources
         BEGIN SELECT RAISE(FAIL, 'injected exclusion failure'); END;",
    )?;
    let turn = start_turn(&runtime, connection, session, "Find a tool").await?;
    assert_eq!(
        completed_turn(&runtime, turn).await?.status,
        TurnStatus::Completed
    );
    assert_eq!(external_context_fact_count(&record.rollout_path)?, 1);
    assert!(
        runtime
            .memory
            .as_ref()
            .context("memory")?
            .scan_source_has_intent(&session.to_string())
            .await
    );
    runtime.shutdown().await;
    Ok(())
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: foreground external-context marking does not wait for the optional memory database's busy timeout.
#[tokio::test]
async fn external_context_marking_does_not_wait_for_busy_memory_storage() -> Result<()> {
    let root = TempDir::new()?;
    let runtime = build_runtime(root.path());
    let connection = initialized_connection(&runtime).await;
    let session = start_durable_session(&runtime, connection, root.path()).await?;
    let record = runtime
        .session(session)
        .await
        .context("session")?
        .record()
        .await
        .flatten()
        .context("record")?;
    rusqlite::Connection::open(root.path().join("connection.db"))?.execute_batch(
        "CREATE TRIGGER reject_external_source BEFORE INSERT ON memory_external_context_sources
         BEGIN SELECT RAISE(FAIL, 'injected source ledger failure'); END;",
    )?;
    let blocker = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
    blocker.execute_batch("BEGIN EXCLUSIVE")?;
    let marked = tokio::time::timeout(
        Duration::from_secs(1),
        runtime.mark_external_context_used(
            Some(record.rollout_path.clone()),
            session,
            /*parent_session_id*/ None,
        ),
    )
    .await;
    // Always release the lock, including when the regression times out.
    blocker.execute_batch("ROLLBACK")?;
    assert!(
        marked.is_ok(),
        "foreground waited for optional memory storage"
    );
    marked?.map_err(anyhow::Error::msg)?;
    assert_eq!(external_context_fact_count(&record.rollout_path)?, 1);
    runtime.shutdown().await;
    Ok(())
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: transient ancestor marker failure while optional memory is absent retains retry ownership across ordinary appends and restart.
#[tokio::test]
async fn unavailable_memory_retries_failed_ancestor_marker_before_restart() -> Result<()> {
    let root = TempDir::new()?;
    std::fs::write(root.path().join("memory"), "unavailable")?;
    let runtime = build_runtime(root.path());
    assert!(runtime.memory.is_none());
    let connection = initialized_connection(&runtime).await;
    let parent = start_durable_session(&runtime, connection, root.path()).await?;
    let record = runtime
        .session(parent)
        .await
        .context("parent")?
        .record()
        .await
        .flatten()
        .context("record")?;
    let backup = root.path().join("rollout-backup.jsonl");
    std::fs::rename(&record.rollout_path, &backup)?;
    std::fs::create_dir(&record.rollout_path)?;
    runtime
        .mark_external_context_used(/*rollout_path*/ None, SessionId::new(), Some(parent))
        .await
        .map_err(anyhow::Error::msg)?;
    std::fs::remove_dir(&record.rollout_path)?;
    std::fs::rename(&backup, &record.rollout_path)?;
    runtime.rollout_store.append_session_meta(&record)?;
    assert_eq!(external_context_fact_count(&record.rollout_path)?, 1);
    runtime.shutdown().await;
    drop(runtime);
    std::fs::remove_file(root.path().join("memory"))?;
    let restarted = build_runtime(root.path());
    let memory = restarted.memory.as_ref().context("memory")?;
    memory.reconcile_source_intents();
    assert!(memory.scan_source_has_intent(&parent.to_string()).await);
    restarted.shutdown().await;
    Ok(())
}
