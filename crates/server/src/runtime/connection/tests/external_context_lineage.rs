use super::*;
use crate::support::{initialize_connection, wait_for_session_notification};
use devo_core::tools::AgentToolCoordinator;
use devo_protocol::native::session::SessionSource;
use pretty_assertions::assert_eq;

async fn start_ephemeral_root(
    runtime: &Arc<ServerRuntime>,
    connection_id: u64,
    cwd: &std::path::Path,
) -> Result<SessionId> {
    let response = runtime
        .start_session_with_registry(
            connection_id,
            serde_json::json!(100),
            SessionStartParams {
                cwd: cwd.to_path_buf(),
                additional_directories: Vec::new(),
                ephemeral: true,
                title: None,
                model: Some("test-model".into()),
                model_binding_id: None,
            },
            /*tool_registry*/ None,
            SessionSource::Interactive,
        )
        .await;
    Ok(
        serde_json::from_value::<crate::SuccessResponse<crate::SessionStartResult>>(response)?
            .result
            .session
            .session_id,
    )
}

pub(super) async fn spawn_and_complete(
    runtime: &Arc<ServerRuntime>,
    notifications: &mut tokio::sync::mpsc::Receiver<serde_json::Value>,
    parent: SessionId,
    message: &str,
) -> Result<SessionId> {
    let child = Arc::clone(runtime)
        .spawn_agent(devo_protocol::SpawnAgentParams {
            session_id: parent,
            message: message.into(),
            fork_turns: Some("none".into()),
            max_turns: None,
            tool_policy: devo_protocol::AgentToolPolicy::Inherit,
            ephemeral: true,
        })
        .await?
        .child_session_id;
    let completed = wait_for_session_notification(notifications, "turn/completed", child).await?;
    assert_eq!(completed["params"]["turn"]["status"], "completed");
    Ok(child)
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: with default-disabled memory, hosted Web remains usable beneath an ephemeral root.
#[tokio::test]
async fn hosted_web_under_ephemeral_root_completes() -> Result<()> {
    let root = TempDir::new()?;
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
    let (connection, mut notifications) = initialize_connection(&runtime).await?;
    let parent = start_ephemeral_root(&runtime, connection, root.path()).await?;
    assert!(runtime.deps.db.get_session_index(&parent)?.is_none());
    spawn_and_complete(&runtime, &mut notifications, parent, "Search the web").await?;
    runtime.shutdown().await;
    Ok(())
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: a wholly ephemeral chain remains usable when memory storage and its source ledger are unavailable.
#[tokio::test]
async fn ephemeral_hosted_web_does_not_require_memory_storage() -> Result<()> {
    let root = TempDir::new()?;
    std::fs::write(root.path().join("memory"), "unavailable memory directory")?;
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
    assert!(runtime.memory.is_none());
    rusqlite::Connection::open(root.path().join("connection.db"))?.execute_batch(
        "CREATE TRIGGER reject_external_source BEFORE INSERT ON memory_external_context_sources
         BEGIN SELECT RAISE(FAIL, 'injected source ledger failure'); END;",
    )?;
    let (connection, mut notifications) = initialize_connection(&runtime).await?;
    let parent = start_ephemeral_root(&runtime, connection, root.path()).await?;
    spawn_and_complete(&runtime, &mut notifications, parent, "Search the web").await?;
    runtime.shutdown().await;
    Ok(())
}

enum RootState {
    Loaded,
    Unloaded,
}

async fn tool_search_through_ephemeral_parent(root_state: RootState) -> Result<()> {
    let root = TempDir::new()?;
    std::fs::write(
        root.path().join("config.toml"),
        "[memory]\nenabled = true\n[tools.web_search]\nmode = 'disabled'\n[tools.web_fetch]\nmode = 'disabled'\n",
    )?;
    let provider = Arc::new(ToolSearchProvider {
        calls: std::sync::atomic::AtomicUsize::new(1),
        tool_name: "ToolSearch",
    });
    let runtime = build_runtime_with_tools(
        root.path(),
        provider.clone(),
        Arc::new(devo_core::tools::create_default_tool_registry()),
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
    let middle = spawn_and_complete(&runtime, &mut notifications, parent, "Prepare").await?;
    assert!(runtime.deps.db.get_session_index(&middle)?.is_none());
    runtime
        .subscribe_connection_to_session(connection, middle, /*event_types*/ None)
        .await;
    if let RootState::Unloaded = root_state {
        runtime
            .remove_session_actor(parent)
            .await
            .context("parent actor")?
            .shutdown()
            .await;
    }
    provider.calls.store(0, std::sync::atomic::Ordering::SeqCst);
    spawn_and_complete(&runtime, &mut notifications, middle, "Find a tool").await?;
    assert_eq!(external_context_fact_count(&record.rollout_path)?, 1);
    runtime
        .memory
        .as_ref()
        .context("memory")?
        .reconcile_source_intents();
    assert!(durable_source_exclusion(root.path(), parent)?);
    runtime.shutdown().await;
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: local external use traverses an ephemeral intermediate agent and excludes its durable root.
#[tokio::test]
async fn tool_search_through_ephemeral_parent_marks_loaded_root() -> Result<()> {
    tool_search_through_ephemeral_parent(RootState::Loaded).await
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: resolving ephemeral ancestors does not lose the exclusion of an unloaded durable root.
#[tokio::test]
async fn tool_search_through_ephemeral_parent_marks_unloaded_root() -> Result<()> {
    tool_search_through_ephemeral_parent(RootState::Unloaded).await
}
