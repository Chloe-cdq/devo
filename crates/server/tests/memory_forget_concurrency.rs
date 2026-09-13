use std::sync::Arc;

use anyhow::{Context, Result};
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::{
    MemoryEntry, MemoryForgetResult, MemoryScope, MemoryState,
};
use devo_protocol::{
    ErrorResponse, ModelRequest, ModelResponse, ProtocolError, ProtocolErrorCode, RequestContent,
    ResponseContent, ResponseMetadata, SessionId, StopReason, StreamEvent, Usage,
};
use devo_server::ServerRuntime;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::sync::mpsc;

#[path = "support/memory_forget.rs"]
#[allow(dead_code)]
mod memory_forget_support;
#[path = "support/subagent_lifecycle.rs"]
#[allow(dead_code)]
mod support;

use memory_forget_support::{BlockingFirstMemoryCommandExecutor, BlockingSecondMemoryListExecutor};
use support::{
    ScriptedProvider, StreamScript, build_runtime_with_overrides, initialize_connection,
    start_parent_session, start_turn_with_approval_policy, wait_for_parent_turn_completed,
};

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-12
/// Verifies: a global direct mutation lease rejects a confirmation from another session until storage completes.
#[tokio::test]
async fn direct_first_blocks_confirmation() -> Result<()> {
    let data_root = configured_data_root()?;
    let provider = Arc::new(ScriptedProvider::new([]));
    let executor = Arc::new(BlockingFirstMemoryCommandExecutor::new());
    let runtime = build_runtime_with_overrides(
        data_root.path(),
        Arc::clone(&provider) as _,
        /*workspace_root*/ Some(data_root.path()),
        /*memory_command_executor*/ Some(Arc::clone(&executor) as _),
    )?;
    let (direct_connection, mut direct_notifications, direct_session) =
        start_subscribed_session(&runtime, data_root.path(), 1).await?;
    let (confirmation_connection, mut confirmation_notifications, confirmation_session) =
        start_subscribed_session(&runtime, data_root.path(), 2).await?;
    let pending = remember(&runtime, direct_connection, 3, "I prefer tabs").await?;
    let direct = remember(&runtime, direct_connection, 4, "My timezone is UTC").await?;
    provider.push_scripts([
        tool_call_script(
            "memory-search",
            "memory_search",
            serde_json::json!({ "query": "tabs" }),
        ),
        ScriptedProvider::completed("select a candidate"),
        tool_call_script(
            "direct-forget",
            "memory_forget",
            serde_json::json!({ "entry_id": direct.entry_id }),
        ),
        tool_call_script(
            "confirmed-forget",
            "memory_forget",
            serde_json::json!({ "entry_id": pending.entry_id }),
        ),
        ScriptedProvider::completed("confirmation blocked"),
        ScriptedProvider::completed("direct complete"),
    ]);

    run_turn(
        &runtime,
        confirmation_connection,
        confirmation_session,
        &mut confirmation_notifications,
        "Find my tab preference",
    )
    .await?;
    let direct_runtime = Arc::clone(&runtime);
    let direct_entry_id = direct.entry_id.clone();
    let direct_task = tokio::spawn(async move {
        run_turn(
            &direct_runtime,
            direct_connection,
            direct_session,
            &mut direct_notifications,
            &format!("Forget memory entry {direct_entry_id}"),
        )
        .await
    });
    executor.wait_until_started().await?;
    run_turn(
        &runtime,
        confirmation_connection,
        confirmation_session,
        &mut confirmation_notifications,
        &format!("Confirm forget memory entry {}", pending.entry_id),
    )
    .await?;
    assert_eq!(
        tool_result(
            provider
                .requests()
                .get(4)
                .context("confirmation result request")?,
            "confirmed-forget",
        ),
        Some("invalid input: memory forget mutation is already in flight")
    );
    executor.release();
    direct_task.await??;
    let active_response = runtime
        .handle_incoming(
            confirmation_connection,
            serde_json::json!({
                "id": 5,
                "method": "memory/list",
                "params": { "scope": MemoryScope::User, "state": MemoryState::Active }
            }),
        )
        .await
        .context("memory/list response")?;
    assert_eq!(
        serde_json::from_value::<Page<MemoryEntry>>(active_response["result"].clone())?,
        Page {
            data: vec![pending],
            next_cursor: None,
        }
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-12
/// Verifies: Native and agent deletion share one global lease in both arrival orders, including across sessions.
#[tokio::test]
async fn native_and_agent_forget_are_mutually_exclusive() -> Result<()> {
    agent_first_blocks_native().await?;
    native_first_blocks_agent().await
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-12
/// Verifies: aborting a real Native forget future drops its reservation and permits a subsequent deletion.
#[tokio::test]
async fn aborted_native_forget_releases_global_lease() -> Result<()> {
    let data_root = TempDir::new()?;
    std::fs::create_dir_all(data_root.path().join(".devo"))?;
    std::fs::write(
        data_root.path().join(".devo").join("config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    let provider = Arc::new(ScriptedProvider::new([]));
    let executor = Arc::new(BlockingFirstMemoryCommandExecutor::new());
    let runtime = build_runtime_with_overrides(
        data_root.path(),
        provider,
        /*workspace_root*/ Some(data_root.path()),
        /*memory_command_executor*/ Some(Arc::clone(&executor) as _),
    )?;
    let (connection_id, _notifications_rx) = initialize_connection(&runtime).await?;
    start_parent_session(&runtime, connection_id, data_root.path()).await?;

    let remembered = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 1,
                "method": "memory/remember",
                "params": { "text": "I prefer tabs", "scope": MemoryScope::User }
            }),
        )
        .await
        .context("memory/remember response")?;
    let entry: MemoryEntry = serde_json::from_value(remembered["result"].clone())?;
    let aborted_runtime = Arc::clone(&runtime);
    let aborted_entry_id = entry.entry_id.clone();
    let aborted = tokio::spawn(async move {
        aborted_runtime
            .handle_incoming(
                connection_id,
                serde_json::json!({
                    "id": 2,
                    "method": "memory/forget",
                    "params": { "entryId": aborted_entry_id }
                }),
            )
            .await
    });
    executor.wait_until_started().await?;

    aborted.abort();
    let cancellation = aborted.await.expect_err("forget future must be cancelled");
    anyhow::ensure!(
        cancellation.is_cancelled(),
        "unexpected join error: {cancellation}"
    );

    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 3,
                "method": "memory/forget",
                "params": { "entryId": entry.entry_id }
            }),
        )
        .await
        .context("retry memory/forget response")?;
    let result: MemoryForgetResult = serde_json::from_value(response["result"].clone())?;
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

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-12
/// Verifies: a real search snapshot completed before a concurrent deletion cannot publish stale pending authority afterward.
#[tokio::test]
async fn deletion_invalidates_search_snapshot_before_pending_publish() -> Result<()> {
    let search_input = serde_json::json!({ "query": "tabs" });
    let provider = Arc::new(ScriptedProvider::new([
        StreamScript::Events(vec![
            StreamEvent::ToolCallStart {
                index: 0,
                id: "memory-search".to_string(),
                name: "memory_search".to_string(),
                input: search_input.clone(),
            },
            StreamEvent::MessageDone {
                response: ModelResponse {
                    id: "memory-search-response".to_string(),
                    content: vec![ResponseContent::ToolUse {
                        id: "memory-search".to_string(),
                        name: "memory_search".to_string(),
                        input: search_input,
                    }],
                    stop_reason: Some(StopReason::ToolUse),
                    usage: Usage::default(),
                    metadata: ResponseMetadata::default(),
                },
            },
        ]),
        ScriptedProvider::completed("stale search rejected"),
    ]));
    let data_root = TempDir::new()?;
    std::fs::create_dir_all(data_root.path().join(".devo"))?;
    std::fs::write(
        data_root.path().join(".devo").join("config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    let executor = Arc::new(BlockingSecondMemoryListExecutor::new());
    let runtime = build_runtime_with_overrides(
        data_root.path(),
        Arc::clone(&provider) as _,
        /*workspace_root*/ Some(data_root.path()),
        /*memory_command_executor*/ Some(Arc::clone(&executor) as _),
    )?;
    let (connection_id, mut notifications_rx) = initialize_connection(&runtime).await?;
    let session_id = start_parent_session(&runtime, connection_id, data_root.path()).await?;
    let subscription = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 10,
                "method": "subscription/create",
                "params": {
                    "selectors": [{ "kind": "session", "sessionId": session_id }],
                    "includeSnapshot": false
                }
            }),
        )
        .await
        .context("subscription/create response")?;
    anyhow::ensure!(
        subscription.get("result").is_some(),
        "subscription/create failed: {subscription}"
    );
    let remembered = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 11,
                "method": "memory/remember",
                "params": { "text": "I prefer tabs", "scope": MemoryScope::User }
            }),
        )
        .await
        .context("memory/remember response")?;
    let entry: MemoryEntry = serde_json::from_value(remembered["result"].clone())?;
    let (native_connection_id, _native_notifications_rx) = initialize_connection(&runtime).await?;
    start_parent_session(&runtime, native_connection_id, data_root.path()).await?;

    start_turn_with_approval_policy(
        &runtime,
        connection_id,
        session_id,
        "Find my tab preference",
        Some("never"),
    )
    .await?;
    executor.wait_until_snapshot_ready().await?;
    let forgotten_response = runtime
        .handle_incoming(
            native_connection_id,
            serde_json::json!({
                "id": 12,
                "method": "memory/forget",
                "params": { "entryId": entry.entry_id }
            }),
        )
        .await
        .context("concurrent memory/forget response")?;
    let forgotten_result: MemoryForgetResult =
        serde_json::from_value(forgotten_response["result"].clone())?;
    let forgotten_entry = forgotten_result
        .forgotten
        .clone()
        .context("forgotten entry")?;
    assert_eq!(
        forgotten_result,
        MemoryForgetResult {
            forgotten: Some(MemoryEntry {
                state: MemoryState::Retired,
                updated_at: forgotten_entry.updated_at,
                ..entry
            }),
            candidates: Vec::new(),
        }
    );
    executor.release();
    wait_for_parent_turn_completed(&mut notifications_rx, session_id).await?;

    let requests = provider.requests();
    let tool_result = requests
        .get(1)
        .context("model request after memory_search")?
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|content| {
            let RequestContent::ToolResult {
                tool_use_id,
                content,
                ..
            } = content
            else {
                return None;
            };
            (tool_use_id == "memory-search").then_some(content.as_str())
        })
        .context("memory_search tool result")?;
    assert_eq!(
        tool_result,
        "invalid input: memory_search snapshot was invalidated by a completed forget mutation"
    );
    Ok(())
}

async fn agent_first_blocks_native() -> Result<()> {
    let data_root = configured_data_root()?;
    let provider = Arc::new(ScriptedProvider::new([]));
    let executor = Arc::new(BlockingFirstMemoryCommandExecutor::new());
    let runtime = build_runtime_with_overrides(
        data_root.path(),
        Arc::clone(&provider) as _,
        /*workspace_root*/ Some(data_root.path()),
        /*memory_command_executor*/ Some(Arc::clone(&executor) as _),
    )?;
    let (agent_connection, mut agent_notifications, agent_session) =
        start_subscribed_session(&runtime, data_root.path(), 20).await?;
    let (native_connection, _native_notifications, _native_session) =
        start_subscribed_session(&runtime, data_root.path(), 21).await?;
    let entry = remember(&runtime, agent_connection, 22, "I prefer tabs").await?;
    provider.push_scripts([
        tool_call_script(
            "agent-forget",
            "memory_forget",
            serde_json::json!({ "entry_id": entry.entry_id }),
        ),
        ScriptedProvider::completed("agent complete"),
    ]);
    let agent_runtime = Arc::clone(&runtime);
    let agent_entry_id = entry.entry_id.clone();
    let agent_task = tokio::spawn(async move {
        run_turn(
            &agent_runtime,
            agent_connection,
            agent_session,
            &mut agent_notifications,
            &format!("Forget memory entry {agent_entry_id}"),
        )
        .await
    });
    executor.wait_until_started().await?;

    let native_response = runtime
        .handle_incoming(
            native_connection,
            serde_json::json!({
                "id": 23,
                "method": "memory/forget",
                "params": { "entryId": entry.entry_id }
            }),
        )
        .await
        .context("concurrent Native memory/forget response")?;
    assert_eq!(
        serde_json::from_value::<ErrorResponse>(native_response)?,
        ErrorResponse {
            id: serde_json::json!(23),
            error: ProtocolError {
                code: ProtocolErrorCode::InvalidParams,
                message: "invalid input: memory forget mutation is already in flight".to_string(),
                data: serde_json::json!({}),
            },
        }
    );
    executor.release();
    agent_task.await??;
    Ok(())
}

async fn native_first_blocks_agent() -> Result<()> {
    let data_root = configured_data_root()?;
    let provider = Arc::new(ScriptedProvider::new([]));
    let executor = Arc::new(BlockingFirstMemoryCommandExecutor::new());
    let runtime = build_runtime_with_overrides(
        data_root.path(),
        Arc::clone(&provider) as _,
        /*workspace_root*/ Some(data_root.path()),
        /*memory_command_executor*/ Some(Arc::clone(&executor) as _),
    )?;
    let (agent_connection, mut agent_notifications, agent_session) =
        start_subscribed_session(&runtime, data_root.path(), 30).await?;
    let (native_connection, _native_notifications, _native_session) =
        start_subscribed_session(&runtime, data_root.path(), 31).await?;
    let entry = remember(&runtime, agent_connection, 32, "I prefer spaces").await?;
    provider.push_scripts([
        tool_call_script(
            "agent-forget",
            "memory_forget",
            serde_json::json!({ "entry_id": entry.entry_id }),
        ),
        ScriptedProvider::completed("agent blocked"),
    ]);
    let native_runtime = Arc::clone(&runtime);
    let native_entry_id = entry.entry_id.clone();
    let native_task = tokio::spawn(async move {
        native_runtime
            .handle_incoming(
                native_connection,
                serde_json::json!({
                    "id": 33,
                    "method": "memory/forget",
                    "params": { "entryId": native_entry_id }
                }),
            )
            .await
            .context("Native memory/forget response")
    });
    executor.wait_until_started().await?;
    run_turn(
        &runtime,
        agent_connection,
        agent_session,
        &mut agent_notifications,
        &format!("Forget memory entry {}", entry.entry_id),
    )
    .await?;
    assert_eq!(
        tool_result(
            provider.requests().get(1).context("agent result request")?,
            "agent-forget",
        ),
        Some("invalid input: memory forget mutation is already in flight")
    );
    executor.release();
    let response = native_task.await??;
    let result: MemoryForgetResult = serde_json::from_value(response["result"].clone())?;
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

fn configured_data_root() -> Result<TempDir> {
    let data_root = TempDir::new()?;
    std::fs::create_dir_all(data_root.path().join(".devo"))?;
    std::fs::write(
        data_root.path().join(".devo").join("config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    Ok(data_root)
}

async fn start_subscribed_session(
    runtime: &Arc<ServerRuntime>,
    workspace_root: &std::path::Path,
    request_id: u64,
) -> Result<(u64, mpsc::Receiver<serde_json::Value>, SessionId)> {
    let (connection_id, notifications) = initialize_connection(runtime).await?;
    let session_id = start_parent_session(runtime, connection_id, workspace_root).await?;
    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": request_id,
                "method": "subscription/create",
                "params": {
                    "selectors": [{ "kind": "session", "sessionId": session_id }],
                    "includeSnapshot": false
                }
            }),
        )
        .await
        .context("subscription/create response")?;
    anyhow::ensure!(
        response.get("result").is_some(),
        "subscription/create failed: {response}"
    );
    Ok((connection_id, notifications, session_id))
}

async fn run_turn(
    runtime: &Arc<ServerRuntime>,
    connection_id: u64,
    session_id: SessionId,
    notifications: &mut mpsc::Receiver<serde_json::Value>,
    text: &str,
) -> Result<()> {
    start_turn_with_approval_policy(runtime, connection_id, session_id, text, Some("never"))
        .await?;
    wait_for_parent_turn_completed(notifications, session_id).await
}

async fn remember(
    runtime: &Arc<ServerRuntime>,
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
                "params": { "text": text, "scope": MemoryScope::User }
            }),
        )
        .await
        .context("memory/remember response")?;
    serde_json::from_value(response["result"].clone())
        .with_context(|| format!("decode memory/remember response: {response}"))
}

fn tool_call_script(id: &str, name: &str, input: serde_json::Value) -> StreamScript {
    StreamScript::Events(vec![
        StreamEvent::ToolCallStart {
            index: 0,
            id: id.to_string(),
            name: name.to_string(),
            input: input.clone(),
        },
        StreamEvent::MessageDone {
            response: ModelResponse {
                id: format!("response-{id}"),
                content: vec![ResponseContent::ToolUse {
                    id: id.to_string(),
                    name: name.to_string(),
                    input,
                }],
                stop_reason: Some(StopReason::ToolUse),
                usage: Usage::default(),
                metadata: ResponseMetadata::default(),
            },
        },
    ])
}

fn tool_result<'a>(request: &'a ModelRequest, tool_use_id: &str) -> Option<&'a str> {
    request
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|content| {
            let RequestContent::ToolResult {
                tool_use_id: result_id,
                content,
                ..
            } = content
            else {
                return None;
            };
            (result_id == tool_use_id).then_some(content.as_str())
        })
}
