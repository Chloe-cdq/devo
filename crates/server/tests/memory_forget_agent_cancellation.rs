use std::sync::Arc;

use anyhow::{Context, Result};
use devo_protocol::SuccessResponse;
use devo_protocol::native::rpc_memory::{MemoryEntry, MemoryForgetResult, MemoryState};
use devo_protocol::native::rpc_session::SessionInterruptResult;
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
use memory_forget_support::BlockingFirstMemoryCommandExecutor;
use support::{
    ScriptedProvider, build_runtime_with_overrides, start_turn_with_approval_policy,
    wait_for_parent_turn_completed,
};

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev4 DD-12
/// Verifies: interrupting a real Agent Confirmed turn releases its lease while preserving the pending candidate for retry.
#[tokio::test]
async fn interrupted_agent_confirmation_preserves_pending_selection_for_retry() -> Result<()> {
    let data_root = configured_data_root()?;
    let provider = Arc::new(ScriptedProvider::new([]));
    let executor = Arc::new(BlockingFirstMemoryCommandExecutor::new());
    let runtime = build_runtime_with_overrides(
        data_root.path(),
        Arc::clone(&provider) as _,
        /*workspace_root*/ Some(data_root.path()),
        /*memory_command_executor*/ Some(Arc::clone(&executor) as _),
    )?;
    let (connection_id, mut notifications, session_id) =
        start_subscribed_session(&runtime, data_root.path(), 1).await?;
    let entry = remember(&runtime, connection_id, 2, "I prefer tabs").await?;
    let entry_id = entry.entry_id.clone();
    provider.push_scripts([
        tool_call_script(
            "memory-search",
            "memory_search",
            serde_json::json!({ "query": "tabs" }),
        ),
        ScriptedProvider::completed("candidate recorded"),
        tool_call_script(
            "interrupted-forget",
            "memory_forget",
            serde_json::json!({ "entry_id": entry_id }),
        ),
        tool_call_script(
            "retry-forget",
            "memory_forget",
            serde_json::json!({ "entry_id": entry_id }),
        ),
        ScriptedProvider::completed("retry complete"),
    ]);
    run_turn(
        &runtime,
        connection_id,
        session_id,
        &mut notifications,
        "Find my tab preference",
    )
    .await?;
    let confirmation = format!("Confirm forget memory entry {entry_id}");
    start_turn_with_approval_policy(
        &runtime,
        connection_id,
        session_id,
        &confirmation,
        Some("never"),
    )
    .await?;
    executor.wait_until_started().await?;

    let interrupt = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 3,
                "method": "session/interrupt",
                "params": {
                    "scope": { "scope": "session", "sessionId": session_id }
                }
            }),
        )
        .await
        .context("session/interrupt response")?;
    assert_eq!(
        serde_json::from_value::<SuccessResponse<SessionInterruptResult>>(interrupt)?,
        SuccessResponse {
            id: serde_json::json!(3),
            result: SessionInterruptResult { interrupted: true },
        }
    );
    wait_for_parent_turn_completed(&mut notifications, session_id).await?;

    run_turn(
        &runtime,
        connection_id,
        session_id,
        &mut notifications,
        &confirmation,
    )
    .await?;
    let requests = provider.requests();
    let result: MemoryForgetResult = serde_json::from_str(
        tool_result(
            requests.get(4).context("retry result request")?,
            "retry-forget",
        )
        .context("retry memory_forget result")?,
    )?;
    let forgotten = result.forgotten.clone().context("forgotten entry")?;
    assert_eq!(
        result,
        MemoryForgetResult {
            forgotten: Some(MemoryEntry {
                state: MemoryState::Retired,
                updated_at: forgotten.updated_at,
                ..entry
            }),
            candidates: Vec::new(),
        }
    );
    Ok(())
}
