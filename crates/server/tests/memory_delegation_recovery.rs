use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use pretty_assertions::assert_eq;

#[path = "support/memory_forget_runtime.rs"]
#[allow(dead_code)]
mod memory_support;
#[path = "support/subagent_lifecycle.rs"]
#[allow(dead_code)]
mod support;

/// Trace: L2-DES-MEM-001 Rev 4 DD-6; L1-REQ-MEM-001.
/// Verifies: a restored spawn approval completes and inherits the original
/// prepared snapshot, including an empty snapshot, despite later store/settings changes.
#[test]
fn restored_spawn_approval_inherits_original_memory() -> Result<()> {
    for recall in ["on", "off"] {
        let data = memory_support::configured_data_root()?;
        let provider = Arc::new(support::ScriptedProvider::new([
            memory_support::tool_call_script(
                "approved-spawn",
                "spawn_agent",
                serde_json::json!({
                    "message": "Use tabs", "fork_turns": "none",
                    "sandbox_permissions": "require_escalated"
                }),
            ),
        ]));
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let (parent, original) = executor.block_on(async {
            let runtime =
                support::build_runtime_with_workspace_config(data.path(), provider.clone())?;
            let (connection, mut notifications, parent) = memory_support::start_subscribed_session(
                &runtime,
                data.path(),
                /*request_id*/ 1,
            )
            .await?;
            memory_support::remember(&runtime, connection, /*request_id*/ 2, "Use tabs").await?;
            let response = runtime
                .handle_incoming(
                    connection,
                    serde_json::json!({
                        "id": 3, "method": "session/metadata/update", "params": {
                            "sessionId": parent, "expectedVersion": 0,
                            "settings": {"memoryRecall": recall}
                        }
                    }),
                )
                .await
                .context("set original recall")?;
            anyhow::ensure!(response.get("result").is_some(), "settings: {response}");
            support::start_turn(&runtime, connection, parent, "Use tabs").await?;
            let approval = next_approval(&mut notifications).await?;
            assert_eq!(
                approval["params"]["approvalId"],
                serde_json::json!("approved-spawn")
            );
            let original = support::message_texts(&provider.requests()[0])
                .into_iter()
                .find(|text| text.contains("<advisory_memory>"));
            assert_eq!(original.is_some(), recall == "on");
            Ok::<_, anyhow::Error>((parent, original))
        })?;
        // A lost executor leaves the real approval checkpoint pending on disk.
        drop(executor);
        provider.push_scripts([
            support::ScriptedProvider::completed("child"),
            support::ScriptedProvider::completed("parent"),
        ]);
        tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
            let runtime = support::build_runtime_with_workspace_config(data.path(), provider.clone())?;
            let (connection, mut notifications) = support::initialize_connection(&runtime).await?;
            let response = runtime.handle_incoming(connection, serde_json::json!({
                "id": 4, "method": "session/resume", "params": {"sessionId": parent}
            })).await.context("restore parent")?;
            anyhow::ensure!(response.get("result").is_some(), "resume: {response}");
            let subscription = runtime.handle_incoming(connection, serde_json::json!({
                "id": 5, "method": "subscription/create", "params": {
                    "selectors": [{"kind": "session", "sessionId": parent}],
                    "includeSnapshot": false
                }
            })).await.context("restore subscription")?;
            anyhow::ensure!(subscription.get("result").is_some(), "subscribe: {subscription}");
            let approval = next_approval(&mut notifications).await?;
            memory_support::remember(&runtime, connection, /*request_id*/ 6, "Use tabs with new advice").await?;
            let response = runtime.handle_incoming(connection, serde_json::json!({
                "id": 7, "method": "session/metadata/update", "params": {
                    "sessionId": parent, "expectedVersion": 0,
                    "settings": {"memoryRecall": if recall == "on" {"off"} else {"on"}}
                }
            })).await.context("change restored recall")?;
            anyhow::ensure!(response.get("result").is_some(), "settings: {response}");
            tokio::time::timeout(Duration::from_secs(/*secs*/ 5), runtime.resolve_client_response(
                connection, serde_json::json!({
                    "jsonrpc": "2.0", "id": approval["id"], "result": {
                        "requestId": "approved-spawn", "decision": {
                            "decision": "approved", "scope": "once", "decidedAt": chrono::Utc::now()
                        }
                    }
                })
            )).await.context("restored spawn approval must not deadlock")?;
            support::wait_for_parent_turn_completed(&mut notifications, parent).await?;
            support::wait_for_stream_calls(&provider, /*expected*/ 3).await?;
            let requests = provider.requests();
            let child_request = requests.iter().skip(/*n*/ 1).find(|request| {
                memory_support::tool_result(request, "approved-spawn").is_none()
            }).context("delegated child model request")?;
            let inherited = support::message_texts(child_request)
                .into_iter().find(|text| text.contains("<advisory_memory>"));
            assert_eq!(inherited, original);
            let parent_request = requests.iter().skip(/*n*/ 1).find(|request| {
                memory_support::tool_result(request, "approved-spawn").is_some()
            }).context("resumed parent model request")?;
            let resumed = support::message_texts(parent_request)
                .into_iter().find(|text| text.contains("<advisory_memory>"));
            assert_eq!(resumed, original);
            runtime.shutdown().await;
            Ok::<_, anyhow::Error>(())
        })?;
    }
    Ok(())
}

async fn next_approval(
    notifications: &mut tokio::sync::mpsc::Receiver<serde_json::Value>,
) -> Result<serde_json::Value> {
    tokio::time::timeout(Duration::from_secs(/*secs*/ 20), async {
        while let Some(event) = notifications.recv().await {
            if event["method"].as_str().is_some_and(|method| {
                method.starts_with("approval/") && method.ends_with("/request")
            }) {
                return Ok(event);
            }
        }
        anyhow::bail!("notification stream closed before approval")
    })
    .await
    .context("waiting for spawn approval")?
}
