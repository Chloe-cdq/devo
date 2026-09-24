use std::sync::Arc;

use anyhow::{Context, Result};
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::{MemoryEntry, MemoryScope, MemoryState};
use devo_protocol::{ErrorResponse, ProtocolError, ProtocolErrorCode};
use pretty_assertions::assert_eq;

#[path = "support/memory_forget_runtime.rs"]
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
use memory_forget_support::BlockingSecondMemoryListExecutor;
use support::{
    ScriptedProvider, build_runtime_with_overrides, start_turn_with_approval_policy,
    wait_for_parent_turn_completed,
};

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev4 DD-12
/// Verifies: a durable forget commit invalidates older search snapshots and consumes confirmation even when projection refresh fails.
#[tokio::test]
async fn durable_commit_with_projection_failure_invalidates_search_and_confirmation() -> Result<()>
{
    let data_root = configured_data_root()?;
    let provider = Arc::new(ScriptedProvider::new([]));
    let executor = Arc::new(BlockingSecondMemoryListExecutor::new());
    let runtime = build_runtime_with_overrides(
        data_root.path(),
        Arc::clone(&provider) as _,
        /*workspace_root*/ Some(data_root.path()),
        /*memory_command_executor*/ Some(Arc::clone(&executor) as _),
    )?;
    let (confirmation_connection, mut confirmation_notifications, confirmation_session) =
        start_subscribed_session(&runtime, data_root.path(), /*request_id*/ 40).await?;
    let (search_connection, mut search_notifications, search_session) =
        start_subscribed_session(&runtime, data_root.path(), /*request_id*/ 41).await?;
    let entry = remember(
        &runtime,
        confirmation_connection,
        /*request_id*/ 42,
        "I prefer tabs",
    )
    .await?;
    let entry_id = entry.entry_id.clone();
    provider.push_scripts([
        tool_call_script(
            "initial-search",
            "memory_search",
            serde_json::json!({ "query": "tabs" }),
        ),
        ScriptedProvider::completed("candidate recorded"),
        tool_call_script(
            "stale-search",
            "memory_search",
            serde_json::json!({ "query": "tabs" }),
        ),
        tool_call_script(
            "confirmed-forget",
            "memory_forget",
            serde_json::json!({ "entry_id": entry_id }),
        ),
        ScriptedProvider::completed("projection failure reported"),
        ScriptedProvider::completed("stale search rejected"),
        tool_call_script(
            "retry-forget",
            "memory_forget",
            serde_json::json!({ "entry_id": entry_id }),
        ),
        ScriptedProvider::completed("retry rejected"),
    ]);
    run_turn(
        &runtime,
        confirmation_connection,
        confirmation_session,
        &mut confirmation_notifications,
        "Find my tab preference",
    )
    .await?;
    let projection = data_root
        .path()
        .join("memory")
        .join("user")
        .join("MEMORY.md");
    std::fs::remove_file(&projection)?;
    std::fs::create_dir(&projection)?;
    executor.block_next_search();
    start_turn_with_approval_policy(
        &runtime,
        search_connection,
        search_session,
        "Find my tab preference again",
        Some("never"),
    )
    .await?;
    executor.wait_until_snapshot_ready().await?;

    run_turn(
        &runtime,
        confirmation_connection,
        confirmation_session,
        &mut confirmation_notifications,
        "Forget the selected memory",
    )
    .await?;
    executor.release();
    wait_for_parent_turn_completed(&mut search_notifications, search_session).await?;
    run_turn(
        &runtime,
        confirmation_connection,
        confirmation_session,
        &mut confirmation_notifications,
        "Forget the selected memory",
    )
    .await?;

    let requests = provider.requests();
    let projection_failure = tool_result(
        requests
            .get(/*result_index*/ 4)
            .context("projection failure result request")?,
        "confirmed-forget",
    )
    .context("confirmed memory_forget result")?;
    anyhow::ensure!(
        projection_failure
            .starts_with("internal error: memory forget committed but projection refresh failed:"),
        "unexpected projection failure result: {projection_failure}"
    );
    assert_eq!(
        tool_result(
            requests
                .get(/*result_index*/ 5)
                .context("stale search result request")?,
            "stale-search"
        ),
        Some(
            "invalid input: memory_search snapshot was invalidated by a completed forget mutation"
        )
    );
    assert_eq!(
        tool_result(
            requests
                .get(/*result_index*/ 7)
                .context("retry result request")?,
            "retry-forget"
        ),
        Some(
            "invalid input: memory_forget requires a current exact stable ID or a pending selection"
        )
    );
    let retired_response = runtime
        .handle_incoming(
            confirmation_connection,
            serde_json::json!({
                "id": 43,
                "method": "memory/list",
                "params": { "scope": MemoryScope::User, "state": MemoryState::Retired }
            }),
        )
        .await
        .context("memory/list retired response")?;
    let retired: Page<MemoryEntry> = serde_json::from_value(retired_response["result"].clone())?;
    let retired_updated_at = retired
        .data
        .first()
        .context("durably forgotten entry")?
        .updated_at;
    assert_eq!(
        retired,
        Page {
            data: vec![MemoryEntry {
                state: MemoryState::Retired,
                updated_at: retired_updated_at,
                ..entry
            }],
            next_cursor: None,
        }
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev4 DD-12
/// Verifies: a Native durable forget commit advances authorization state even when projection refresh fails.
#[tokio::test]
async fn native_durable_commit_with_projection_failure_invalidates_search_and_confirmation()
-> Result<()> {
    let data_root = configured_data_root()?;
    let provider = Arc::new(ScriptedProvider::new([]));
    let executor = Arc::new(BlockingSecondMemoryListExecutor::new());
    let runtime = build_runtime_with_overrides(
        data_root.path(),
        Arc::clone(&provider) as _,
        /*workspace_root*/ Some(data_root.path()),
        /*memory_command_executor*/ Some(Arc::clone(&executor) as _),
    )?;
    let (pending_connection, mut pending_notifications, pending_session) =
        start_subscribed_session(&runtime, data_root.path(), /*request_id*/ 50).await?;
    let (search_connection, mut search_notifications, search_session) =
        start_subscribed_session(&runtime, data_root.path(), /*request_id*/ 51).await?;
    let (native_connection, _native_notifications, _native_session) =
        start_subscribed_session(&runtime, data_root.path(), /*request_id*/ 52).await?;
    let entry = remember(
        &runtime,
        pending_connection,
        /*request_id*/ 53,
        "I prefer tabs",
    )
    .await?;
    let entry_id = entry.entry_id.clone();
    provider.push_scripts([
        tool_call_script(
            "initial-search",
            "memory_search",
            serde_json::json!({ "query": "tabs" }),
        ),
        ScriptedProvider::completed("candidate recorded"),
        tool_call_script(
            "stale-search",
            "memory_search",
            serde_json::json!({ "query": "tabs" }),
        ),
        ScriptedProvider::completed("stale search rejected"),
        tool_call_script(
            "retry-forget",
            "memory_forget",
            serde_json::json!({ "entry_id": entry_id }),
        ),
        ScriptedProvider::completed("retry rejected"),
    ]);
    run_turn(
        &runtime,
        pending_connection,
        pending_session,
        &mut pending_notifications,
        "Find my tab preference",
    )
    .await?;
    let projection = data_root
        .path()
        .join("memory")
        .join("user")
        .join("MEMORY.md");
    std::fs::remove_file(&projection)?;
    std::fs::create_dir(&projection)?;
    executor.block_next_search();
    start_turn_with_approval_policy(
        &runtime,
        search_connection,
        search_session,
        "Find my tab preference again",
        Some("never"),
    )
    .await?;
    executor.wait_until_snapshot_ready().await?;

    let native_response = runtime
        .handle_incoming(
            native_connection,
            serde_json::json!({
                "id": 54,
                "method": "memory/forget",
                "params": { "entryId": entry_id }
            }),
        )
        .await
        .context("Native memory/forget response")?;
    let native_error: ErrorResponse = serde_json::from_value(native_response)?;
    let projection_failure = native_error.error.message.clone();
    assert_eq!(
        native_error,
        ErrorResponse {
            id: serde_json::json!(54),
            error: ProtocolError {
                code: ProtocolErrorCode::InternalError,
                message: projection_failure.clone(),
                data: serde_json::json!({}),
            },
        }
    );
    anyhow::ensure!(
        projection_failure.starts_with("memory forget committed but projection refresh failed:"),
        "unexpected Native projection failure: {projection_failure}"
    );
    executor.release();
    wait_for_parent_turn_completed(&mut search_notifications, search_session).await?;
    run_turn(
        &runtime,
        pending_connection,
        pending_session,
        &mut pending_notifications,
        "Forget the selected memory",
    )
    .await?;

    let requests = provider.requests();
    assert_eq!(
        tool_result(
            requests
                .get(/*result_index*/ 3)
                .context("stale search result request")?,
            "stale-search"
        ),
        Some(
            "invalid input: memory_search snapshot was invalidated by a completed forget mutation"
        )
    );
    assert_eq!(
        tool_result(
            requests
                .get(/*result_index*/ 5)
                .context("retry result request")?,
            "retry-forget"
        ),
        Some(
            "invalid input: memory_forget requires a current exact stable ID or a pending selection"
        )
    );
    let retired_response = runtime
        .handle_incoming(
            native_connection,
            serde_json::json!({
                "id": 55,
                "method": "memory/list",
                "params": { "scope": MemoryScope::User, "state": MemoryState::Retired }
            }),
        )
        .await
        .context("memory/list retired response")?;
    let retired: Page<MemoryEntry> = serde_json::from_value(retired_response["result"].clone())?;
    let retired_updated_at = retired
        .data
        .first()
        .context("durably forgotten Native entry")?
        .updated_at;
    assert_eq!(
        retired,
        Page {
            data: vec![MemoryEntry {
                state: MemoryState::Retired,
                updated_at: retired_updated_at,
                ..entry
            }],
            next_cursor: None,
        }
    );
    Ok(())
}
