use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use devo_protocol::native::rpc_memory::{MemoryEntry, MemoryScope};
use devo_protocol::{ErrorResponse, ProtocolError, ProtocolErrorCode};
use devo_server::memory::{
    MemoryCommand, MemoryCommandExecutor, MemoryCommandResult, MemoryError, MemoryRuntime,
};
use pretty_assertions::assert_eq;

#[path = "support/memory_forget_runtime.rs"]
#[allow(dead_code)]
mod memory_forget_runtime_support;
#[path = "support/memory_forget.rs"]
#[allow(dead_code)]
mod memory_forget_support;
#[path = "support/subagent_lifecycle.rs"]
#[allow(dead_code)]
mod support;

use memory_forget_runtime_support::{
    configured_data_root, remember, run_turn, start_subscribed_session, tool_call_script,
    tool_result,
};
use memory_forget_support::BlockingFirstMemoryCommandExecutor;
use support::{ScriptedProvider, build_runtime_with_overrides};

struct RejectPrepareForgetExecutor;

#[async_trait]
impl MemoryCommandExecutor for RejectPrepareForgetExecutor {
    async fn execute(
        &self,
        memory: &MemoryRuntime,
        command: MemoryCommand,
    ) -> Result<MemoryCommandResult, MemoryError> {
        if matches!(command, MemoryCommand::PrepareForget(_)) {
            return Err(MemoryError::InvalidRequest(
                "prepare forget was intercepted".to_string(),
            ));
        }
        memory.execute_command(command).await
    }
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev4 DD-13
/// Verifies: Native target lookup enters Memory through the command executor seam.
#[tokio::test]
async fn native_forget_preparation_uses_command_executor() -> Result<()> {
    const SUBSCRIPTION_REQUEST_ID: u64 = 10;
    const FORGET_REQUEST_ID: u64 = 11;

    let data_root = configured_data_root()?;
    let provider = Arc::new(ScriptedProvider::new([]));
    let runtime = build_runtime_with_overrides(
        data_root.path(),
        Arc::clone(&provider) as _,
        /*workspace_root*/ Some(data_root.path()),
        /*memory_command_executor*/ Some(Arc::new(RejectPrepareForgetExecutor)),
    )?;
    let (connection_id, _notifications, _session_id) = start_subscribed_session(
        &runtime,
        data_root.path(),
        /*request_id*/ SUBSCRIPTION_REQUEST_ID,
    )
    .await?;

    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": FORGET_REQUEST_ID,
                "method": "memory/forget",
                "params": { "entryId": "mem_missing" }
            }),
        )
        .await
        .context("memory/forget response")?;
    assert_eq!(
        serde_json::from_value::<ErrorResponse>(response)?,
        ErrorResponse {
            id: serde_json::json!(FORGET_REQUEST_ID),
            error: ProtocolError {
                code: ProtocolErrorCode::InvalidParams,
                message: "prepare forget was intercepted".to_string(),
                data: serde_json::json!({}),
            },
        }
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev4 DD-13
/// Verifies: Agent target lookup enters Memory through the command executor seam.
#[tokio::test]
async fn agent_forget_preparation_uses_command_executor() -> Result<()> {
    const SUBSCRIPTION_REQUEST_ID: u64 = 20;

    let data_root = configured_data_root()?;
    let provider = Arc::new(ScriptedProvider::new([
        tool_call_script(
            "missing-forget",
            "memory_forget",
            serde_json::json!({ "entry_id": "mem_missing" }),
        ),
        ScriptedProvider::completed("Forget rejected"),
    ]));
    let runtime = build_runtime_with_overrides(
        data_root.path(),
        Arc::clone(&provider) as _,
        /*workspace_root*/ Some(data_root.path()),
        /*memory_command_executor*/ Some(Arc::new(RejectPrepareForgetExecutor)),
    )?;
    let (connection_id, mut notifications, session_id) = start_subscribed_session(
        &runtime,
        data_root.path(),
        /*request_id*/ SUBSCRIPTION_REQUEST_ID,
    )
    .await?;

    run_turn(
        &runtime,
        connection_id,
        session_id,
        &mut notifications,
        "Forget memory entry mem_missing",
    )
    .await?;
    assert_eq!(
        tool_result(
            provider
                .requests()
                .get(/*result_index*/ 1)
                .context("memory forget result request")?,
            "missing-forget",
        ),
        Some("invalid input: prepare forget was intercepted")
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev4 DD-12
/// Verifies: Agent Project identity validation finishes before forget attempts to acquire its lease.
#[tokio::test]
async fn agent_project_forget_resolves_identity_before_lease() -> Result<()> {
    const SUBSCRIPTION_REQUEST_ID: u64 = 30;
    const USER_REMEMBER_REQUEST_ID: u64 = 31;
    const PROJECT_REMEMBER_REQUEST_ID: u64 = 32;
    const USER_FORGET_REQUEST_ID: u64 = 33;

    let data_root = configured_data_root()?;
    let workspace_root = data_root.path().join("removed-project");
    std::fs::create_dir_all(workspace_root.join(".devo"))?;
    std::fs::write(
        workspace_root.join(".devo").join("config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    let provider = Arc::new(ScriptedProvider::new([]));
    let executor = Arc::new(BlockingFirstMemoryCommandExecutor::new());
    let runtime = build_runtime_with_overrides(
        data_root.path(),
        Arc::clone(&provider) as _,
        /*workspace_root*/ Some(&workspace_root),
        /*memory_command_executor*/ Some(Arc::clone(&executor) as _),
    )?;
    let (connection_id, mut notifications, session_id) = start_subscribed_session(
        &runtime,
        &workspace_root,
        /*request_id*/ SUBSCRIPTION_REQUEST_ID,
    )
    .await?;
    let user_entry = remember(
        &runtime,
        connection_id,
        /*request_id*/ USER_REMEMBER_REQUEST_ID,
        "I prefer tabs",
    )
    .await?;
    let project_entry = remember_project(
        &runtime,
        connection_id,
        /*request_id*/ PROJECT_REMEMBER_REQUEST_ID,
        "The project uses tabs",
    )
    .await?;
    provider.push_scripts([
        tool_call_script(
            "project-forget",
            "memory_forget",
            serde_json::json!({ "entry_id": project_entry.entry_id }),
        ),
        ScriptedProvider::completed("Project forget rejected"),
    ]);

    let held_runtime = Arc::clone(&runtime);
    let held_forget = tokio::spawn(async move {
        held_runtime
            .handle_incoming(
                connection_id,
                serde_json::json!({
                    "id": USER_FORGET_REQUEST_ID,
                    "method": "memory/forget",
                    "params": { "entryId": user_entry.entry_id }
                }),
            )
            .await
    });
    executor.wait_until_started().await?;
    std::fs::remove_dir_all(&workspace_root)?;

    run_turn(
        &runtime,
        connection_id,
        session_id,
        &mut notifications,
        &format!("Forget memory entry {}", project_entry.entry_id),
    )
    .await?;

    assert_eq!(
        tool_result(
            provider
                .requests()
                .get(/*result_index*/ 1)
                .context("Project forget result request")?,
            "project-forget",
        ),
        Some("internal error: memory operation is unavailable")
    );
    executor.release();
    held_forget.await?.context("User memory/forget response")?;
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev4 DD-12
/// Verifies: Agent exact-ID Project mismatch is rejected before lease admission.
#[tokio::test]
async fn agent_wrong_project_is_rejected_before_lease() -> Result<()> {
    const PROJECT_A_SUBSCRIPTION_REQUEST_ID: u64 = 40;
    const PROJECT_REMEMBER_REQUEST_ID: u64 = 41;
    const PROJECT_B_SUBSCRIPTION_REQUEST_ID: u64 = 42;
    const USER_REMEMBER_REQUEST_ID: u64 = 43;
    const USER_FORGET_REQUEST_ID: u64 = 44;

    let data_root = configured_data_root()?;
    let project_a = data_root.path().join("project-a");
    let project_b = data_root.path().join("project-b");
    std::fs::create_dir_all(&project_a)?;
    std::fs::create_dir_all(&project_b)?;
    let provider = Arc::new(ScriptedProvider::new([]));
    let executor = Arc::new(BlockingFirstMemoryCommandExecutor::new());
    let runtime = build_runtime_with_overrides(
        data_root.path(),
        Arc::clone(&provider) as _,
        /*workspace_root*/ Some(data_root.path()),
        /*memory_command_executor*/ Some(Arc::clone(&executor) as _),
    )?;
    let (project_a_connection, _project_a_notifications, _project_a_session) =
        start_subscribed_session(
            &runtime,
            &project_a,
            /*request_id*/ PROJECT_A_SUBSCRIPTION_REQUEST_ID,
        )
        .await?;
    let project_entry = remember_project(
        &runtime,
        project_a_connection,
        /*request_id*/ PROJECT_REMEMBER_REQUEST_ID,
        "Project A uses tabs",
    )
    .await?;
    let (project_b_connection, mut project_b_notifications, project_b_session) =
        start_subscribed_session(
            &runtime,
            &project_b,
            /*request_id*/ PROJECT_B_SUBSCRIPTION_REQUEST_ID,
        )
        .await?;
    let user_entry = remember(
        &runtime,
        project_b_connection,
        /*request_id*/ USER_REMEMBER_REQUEST_ID,
        "I prefer tabs",
    )
    .await?;
    provider.push_scripts([
        tool_call_script(
            "wrong-project-forget",
            "memory_forget",
            serde_json::json!({ "entry_id": project_entry.entry_id }),
        ),
        ScriptedProvider::completed("Project forget rejected"),
    ]);

    let held_runtime = Arc::clone(&runtime);
    let held_forget = tokio::spawn(async move {
        held_runtime
            .handle_incoming(
                project_b_connection,
                serde_json::json!({
                    "id": USER_FORGET_REQUEST_ID,
                    "method": "memory/forget",
                    "params": { "entryId": user_entry.entry_id }
                }),
            )
            .await
    });
    executor.wait_until_started().await?;

    run_turn(
        &runtime,
        project_b_connection,
        project_b_session,
        &mut project_b_notifications,
        &format!("Forget memory entry {}", project_entry.entry_id),
    )
    .await?;

    assert_eq!(
        tool_result(
            provider
                .requests()
                .get(/*result_index*/ 1)
                .context("wrong Project forget result request")?,
            "wrong-project-forget",
        ),
        Some("invalid input: memory entry not found")
    );
    executor.release();
    held_forget.await?.context("User memory/forget response")?;
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev4 DD-12
/// Verifies: Native exact-ID Project mismatch is rejected before lease admission.
#[tokio::test]
async fn native_wrong_project_is_rejected_before_lease() -> Result<()> {
    const PROJECT_A_SUBSCRIPTION_REQUEST_ID: u64 = 50;
    const PROJECT_REMEMBER_REQUEST_ID: u64 = 51;
    const PROJECT_B_SUBSCRIPTION_REQUEST_ID: u64 = 52;
    const USER_REMEMBER_REQUEST_ID: u64 = 53;
    const USER_FORGET_REQUEST_ID: u64 = 54;
    const PROJECT_FORGET_REQUEST_ID: u64 = 55;

    let data_root = configured_data_root()?;
    let project_a = data_root.path().join("project-a");
    let project_b = data_root.path().join("project-b");
    std::fs::create_dir_all(&project_a)?;
    std::fs::create_dir_all(&project_b)?;
    let provider = Arc::new(ScriptedProvider::new([]));
    let executor = Arc::new(BlockingFirstMemoryCommandExecutor::new());
    let runtime = build_runtime_with_overrides(
        data_root.path(),
        Arc::clone(&provider) as _,
        /*workspace_root*/ Some(data_root.path()),
        /*memory_command_executor*/ Some(Arc::clone(&executor) as _),
    )?;
    let (project_a_connection, _project_a_notifications, _project_a_session) =
        start_subscribed_session(
            &runtime,
            &project_a,
            /*request_id*/ PROJECT_A_SUBSCRIPTION_REQUEST_ID,
        )
        .await?;
    let project_entry = remember_project(
        &runtime,
        project_a_connection,
        /*request_id*/ PROJECT_REMEMBER_REQUEST_ID,
        "Project A uses tabs",
    )
    .await?;
    let (project_b_connection, _project_b_notifications, _project_b_session) =
        start_subscribed_session(
            &runtime,
            &project_b,
            /*request_id*/ PROJECT_B_SUBSCRIPTION_REQUEST_ID,
        )
        .await?;
    let user_entry = remember(
        &runtime,
        project_b_connection,
        /*request_id*/ USER_REMEMBER_REQUEST_ID,
        "I prefer tabs",
    )
    .await?;

    let held_runtime = Arc::clone(&runtime);
    let held_forget = tokio::spawn(async move {
        held_runtime
            .handle_incoming(
                project_b_connection,
                serde_json::json!({
                    "id": USER_FORGET_REQUEST_ID,
                    "method": "memory/forget",
                    "params": { "entryId": user_entry.entry_id }
                }),
            )
            .await
    });
    executor.wait_until_started().await?;

    let response = runtime
        .handle_incoming(
            project_b_connection,
            serde_json::json!({
                "id": PROJECT_FORGET_REQUEST_ID,
                "method": "memory/forget",
                "params": { "entryId": project_entry.entry_id }
            }),
        )
        .await
        .context("wrong Project memory/forget response")?;
    assert_eq!(
        serde_json::from_value::<ErrorResponse>(response)?,
        ErrorResponse {
            id: serde_json::json!(PROJECT_FORGET_REQUEST_ID),
            error: ProtocolError {
                code: ProtocolErrorCode::InvalidParams,
                message: "memory entry not found".to_string(),
                data: serde_json::json!({}),
            },
        }
    );
    executor.release();
    held_forget.await?.context("User memory/forget response")?;
    Ok(())
}

async fn remember_project(
    runtime: &Arc<devo_server::ServerRuntime>,
    connection_id: u64,
    request_id: u64,
    text: &str,
) -> Result<MemoryEntry> {
    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": request_id,
                "method": "memory/remember",
                "params": {
                    "text": text,
                    "scope": MemoryScope::Project,
                }
            }),
        )
        .await
        .context("Project memory/remember response")?;
    serde_json::from_value(response["result"].clone()).context("decode Project memory entry")
}
