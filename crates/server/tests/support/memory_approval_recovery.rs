use super::*;
use pretty_assertions::assert_eq;

/// Trace: L2-DES-MEM-001 Rev 4 DD-6, L2-DES-CONTEXT-004.
/// Verifies: a real persisted approval checkpoint retains memory source binding
/// after its executor disappears, without duplicating user history or evidence.
#[test]
fn approval_checkpoint_recovery_preserves_memory_source() -> Result<()> {
    let data_root = memory_forget_runtime_support::configured_data_root()?;
    let provider = Arc::new(support::ScriptedProvider::new([
        memory_forget_runtime_support::tool_call_script(
            "approval-before-memory",
            "exec_command",
            serde_json::json!({
                "cmd": "echo approved", "login": false,
                "sandbox_permissions": "require_escalated",
                "justification": "Exercise approval checkpoint recovery",
                "yield_time_ms": 1000, "max_output_tokens": 1000
            }),
        ),
    ]));
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (session_id, turn_id, original_user_items, forget_target) = executor.block_on(async {
        let runtime = support::build_runtime_with_workspace_config(data_root.path(), Arc::clone(&provider) as _)?;
        let (connection_id, mut notifications, session_id) =
            memory_forget_runtime_support::start_subscribed_session(&runtime, data_root.path(), 40).await?;
        let forget_target = memory_forget_runtime_support::remember(
            &runtime, connection_id, 41, "I prefer tabs for indentation.",
        ).await?;
        let started = support::start_turn(&runtime, connection_id, session_id,
            &format!("Run the approved command. Remember that I prefer concise explanations. Forget memory entry {}.", forget_target.entry_id),
        ).await?;
        let request = next_approval(&mut notifications).await.context("initial live approval")?;
        assert_eq!(request["params"]["approvalId"], serde_json::json!("approval-before-memory"));
        let items = user_items(&runtime, connection_id, session_id).await?;
        assert_eq!(items.len(), 1);
        Ok::<_, anyhow::Error>((session_id, started.turn.id, items, forget_target))
    })?;
    // Dropping the executor aborts the blocked turn without writing graceful
    // shutdown decisions. The next runtime reads the actual durable checkpoint.
    drop(executor);

    provider.push_scripts([
        memory_forget_runtime_support::tool_call_script(
            "remember-after-approval",
            "memory_remember",
            serde_json::json!({ "text": "I prefer concise explanations." }),
        ),
        memory_forget_runtime_support::tool_call_script(
            "search-after-approval",
            "memory_search",
            serde_json::json!({ "query": "tabs" }),
        ),
        memory_forget_runtime_support::tool_call_script(
            "forget-after-approval",
            "memory_forget",
            serde_json::json!({ "entry_id": forget_target.entry_id }),
        ),
        support::ScriptedProvider::completed("Remembered after approval."),
    ]);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let runtime = support::build_runtime_with_workspace_config(
                data_root.path(),
                Arc::clone(&provider) as _,
            )?;
            let (connection_id, mut notifications) =
                support::initialize_connection(&runtime).await?;
            let response = runtime
                .handle_incoming(
                    connection_id,
                    serde_json::json!({
                        "id": 42, "method": "session/resume", "params": { "sessionId": session_id }
                    }),
                )
                .await
                .context("session/resume")?;
            anyhow::ensure!(response.get("result").is_some(), "resume: {response}");
            let subscription = runtime
                .handle_incoming(
                    connection_id,
                    serde_json::json!({
                        "id": 43, "method": "subscription/create", "params": {
                            "selectors": [{ "kind": "session", "sessionId": session_id }],
                            "includeSnapshot": false
                        }
                    }),
                )
                .await
                .context("restore Native subscription")?;
            anyhow::ensure!(
                subscription.get("result").is_some(),
                "subscription: {subscription}"
            );
            let request = next_approval(&mut notifications)
                .await
                .context("restored approval")?;
            assert_eq!(
                request["params"]["approvalId"],
                serde_json::json!("approval-before-memory")
            );
            runtime
                .resolve_client_response(
                    connection_id,
                    serde_json::json!({
                        "jsonrpc": "2.0", "id": request["id"],
                        "result": { "requestId": "approval-before-memory", "decision": {
                            "decision": "approved", "scope": "once", "decidedAt": chrono::Utc::now()
                        } }
                    }),
                )
                .await;
            // Restoring a checkpoint runs real shell and durable memory I/O;
            // this source-binding test uses the same budget as its approval wait.
            let mut observed = Vec::new();
            let completed = tokio::time::timeout(Duration::from_secs(/*secs*/ 20), async {
                while let Some(value) = notifications.recv().await {
                    if value["method"] == "turn/completed"
                        && value["params"]["turn"]["sessionId"] == serde_json::json!(session_id)
                        && value["params"]["turn"]["id"] == serde_json::json!(turn_id)
                    {
                        return Ok::<_, anyhow::Error>(value);
                    }
                    observed.push(value);
                }
                anyhow::bail!("notification stream closed before restored turn completed")
            })
            .await
            .with_context(|| {
                format!("wait for restored approval checkpoint turn; observed: {observed:?}")
            })??;
            assert_eq!(
                completed["params"]["turn"]["status"],
                serde_json::json!("completed")
            );
            let requests = provider.requests();
            let final_request = requests.last().context("resumed model request")?;
            let approved_command =
                memory_forget_runtime_support::tool_result(final_request, "approval-before-memory")
                    .context("restored pending command result")?;
            // Exec can legitimately yield a live process before its output arrives.
            // This checkpoint verifies restored authorization, not shell startup time.
            let decoded_command = approved_command
                .strip_prefix("Text(")
                .and_then(|text| text.strip_suffix(')'))
                .map(serde_json::from_str::<String>)
                .transpose()?;
            let approved_command = decoded_command.as_deref().unwrap_or(approved_command);
            let running = approved_command.lines().any(|line| {
                line.strip_prefix("Process running with process ID ")
                    .and_then(|id| id.parse::<i32>().ok())
                    .is_some_and(|id| id > 0)
            });
            anyhow::ensure!(
                (approved_command.contains("Process exited with code 0")
                    && approved_command.contains("approved"))
                    || running,
                "restored approved command did not execute: {approved_command}"
            );
            let remembered: MemoryEntry = serde_json::from_str(
                memory_forget_runtime_support::tool_result(
                    final_request,
                    "remember-after-approval",
                )
                .context("remember result")?,
            )?;
            assert_memory_evidence(
                data_root.path(),
                &remembered.entry_id.to_string(),
                &session_id.to_string(),
                &turn_id.to_string(),
                original_user_items[0].id.as_str(),
            )?;
            let searched: MemorySearchResult = serde_json::from_str(
                memory_forget_runtime_support::tool_result(final_request, "search-after-approval")
                    .context("search result")?,
            )?;
            assert_eq!(
                searched,
                Page {
                    data: vec![MemorySearchEntry {
                        entry_id: forget_target.entry_id.clone(),
                        scope: forget_target.scope,
                        kind: forget_target.kind,
                        state: MemoryState::Active,
                        summary: forget_target.body.clone()
                    }],
                    next_cursor: None
                }
            );
            let forgotten: MemoryForgetResult = serde_json::from_str(
                memory_forget_runtime_support::tool_result(final_request, "forget-after-approval")
                    .context("forget result")?,
            )?;
            let updated_at = forgotten
                .forgotten
                .as_ref()
                .context("retired entry")?
                .updated_at;
            assert_eq!(
                forgotten,
                MemoryForgetResult {
                    forgotten: Some(MemoryEntry {
                        state: MemoryState::Retired,
                        updated_at,
                        ..forget_target
                    }),
                    candidates: Vec::new()
                }
            );
            assert_eq!(
                user_items(&runtime, connection_id, session_id).await?,
                original_user_items
            );
            runtime.shutdown().await;
            Ok(())
        })
}

async fn next_approval(
    notifications: &mut tokio::sync::mpsc::Receiver<serde_json::Value>,
) -> Result<serde_json::Value> {
    let mut observed = Vec::new();
    tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(value) = notifications.recv().await {
            if value["method"].as_str().is_some_and(|method| {
                method.starts_with("approval/") && method.ends_with("/request")
            }) {
                return Ok(value);
            }
            observed.push(value);
        }
        anyhow::bail!("notification stream closed before approval")
    })
    .await
    .with_context(|| format!("waiting for checkpoint approval; observed: {observed:?}"))?
}
