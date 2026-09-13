use std::sync::Arc;

use anyhow::{Context, Result};
use devo_protocol::{
    ModelRequest, ModelResponse, RequestContent, ResponseContent, ResponseMetadata, SessionId,
    StopReason, StreamEvent, Usage,
};
use devo_server::ServerRuntime;
use tempfile::TempDir;
use tokio::sync::mpsc;

use crate::support::{
    StreamScript, initialize_connection, start_parent_session, start_turn_with_approval_policy,
    wait_for_parent_turn_completed,
};

pub fn configured_data_root() -> Result<TempDir> {
    let data_root = TempDir::new()?;
    std::fs::create_dir_all(data_root.path().join(".devo"))?;
    std::fs::write(
        data_root.path().join(".devo").join("config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    Ok(data_root)
}

pub async fn start_subscribed_session(
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

pub async fn run_turn(
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

pub async fn remember(
    runtime: &Arc<ServerRuntime>,
    connection_id: u64,
    request_id: u64,
    text: &str,
) -> Result<devo_protocol::native::rpc_memory::MemoryEntry> {
    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": request_id,
                "method": "memory/remember",
                "params": {
                    "text": text,
                    "scope": devo_protocol::native::rpc_memory::MemoryScope::User
                }
            }),
        )
        .await
        .context("memory/remember response")?;
    serde_json::from_value(response["result"].clone())
        .with_context(|| format!("decode memory/remember response: {response}"))
}

pub fn tool_call_script(id: &str, name: &str, input: serde_json::Value) -> StreamScript {
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

pub fn tool_result<'a>(request: &'a ModelRequest, tool_use_id: &str) -> Option<&'a str> {
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
