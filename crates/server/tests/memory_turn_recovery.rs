use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use devo_protocol::native::item::{Item, ItemEnvelope};
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::{
    MemoryEntry, MemoryForgetResult, MemorySearchEntry, MemorySearchResult, MemoryState,
};
use pretty_assertions::assert_eq;

#[path = "support/memory_approval_recovery.rs"]
mod approval_recovery;

#[path = "support/memory_forget_runtime.rs"]
#[allow(dead_code)]
mod memory_forget_runtime_support;
#[path = "support/subagent_lifecycle.rs"]
#[allow(dead_code)]
mod support;

use memory_forget_runtime_support::model_events;

/// Trace: L2-DES-MEM-001 Rev 4 DD-6; L2-DES-CONTEXT-004.
/// A recovered root turn must retain its authoritative current user message
/// so the actual memory tool handler can validate and commit explicit intent.
#[tokio::test(start_paused = true)]
async fn resumed_turn_memory_tools_keep_original_user_item() -> Result<()> {
    for restart_runtime in [false, true] {
        let data_root = memory_forget_runtime_support::configured_data_root()?;
        // An empty script queue makes every request fail until recovery is offered.
        // This avoids relying on the core's current retry-attempt count.
        let provider = Arc::new(support::ScriptedProvider::new([]));
        let mut runtime = support::build_runtime_with_workspace_config(
            data_root.path(),
            Arc::clone(&provider) as _,
        )?;
        let (mut connection_id, mut notifications, session_id) =
            memory_forget_runtime_support::start_subscribed_session(
                &runtime,
                data_root.path(),
                /*request_id*/ 30,
            )
            .await?;
        let forget_target = memory_forget_runtime_support::remember(
            &runtime,
            connection_id,
            /*request_id*/ 29,
            "I prefer tabs for indentation.",
        )
        .await?;
        let started = support::start_turn_with_approval_policy(
            &runtime,
            connection_id,
            session_id,
            &format!(
                "Remember that I prefer concise explanations. Forget memory entry {}.",
                forget_target.entry_id
            ),
            Some("never"),
        )
        .await?;
        let recovery = tokio::time::timeout(Duration::from_secs(120), async {
            while let Some(event) = notifications.recv().await {
                if event["method"] == "turn/recoveryUpdated"
                    && event["params"]["recovery"].is_object()
                {
                    return Ok::<_, anyhow::Error>(event["params"]["recovery"].clone());
                }
            }
            anyhow::bail!("notification stream closed before recovery became available")
        })
        .await
        .context("wait for recoverable provider failure")??;
        assert_eq!(recovery["turnId"], serde_json::json!(started.turn.id));
        let original_user_items = user_items(&runtime, connection_id, session_id).await?;
        assert_eq!(original_user_items.len(), 1);

        if restart_runtime {
            runtime.shutdown().await;
            drop(runtime);
            runtime = support::build_runtime_with_workspace_config(
                data_root.path(),
                Arc::clone(&provider) as _,
            )?;
            (connection_id, notifications) = support::initialize_connection(&runtime).await?;
            let resumed_session = runtime
                .handle_incoming(
                    connection_id,
                    serde_json::json!({
                        "id": 34, "method": "session/resume", "params": { "sessionId": session_id }
                    }),
                )
                .await
                .context("session/resume after restart")?;
            anyhow::ensure!(
                resumed_session.get("result").is_some(),
                "session/resume failed: {resumed_session}"
            );
            let subscription = runtime
                .handle_incoming(
                    connection_id,
                    serde_json::json!({
                        "id": 35,
                        "method": "subscription/create",
                        "params": {
                            "selectors": [{ "kind": "session", "sessionId": session_id }],
                            "includeSnapshot": false
                        }
                    }),
                )
                .await
                .context("subscription/create after restart")?;
            anyhow::ensure!(
                subscription.get("result").is_some(),
                "subscription/create failed: {subscription}"
            );
        }
        provider.push_scripts([
            support::StreamScript::Events(model_events::tool_call_events(
                "remember-after-recovery",
                "memory_remember",
                serde_json::json!({ "text": "I prefer concise explanations." }),
            )),
            support::StreamScript::Events(model_events::tool_call_events(
                "search-after-recovery",
                "memory_search",
                serde_json::json!({ "query": "tabs" }),
            )),
            support::StreamScript::Events(model_events::tool_call_events(
                "forget-after-recovery",
                "memory_forget",
                serde_json::json!({ "entry_id": forget_target.entry_id }),
            )),
            support::ScriptedProvider::completed("Remembered."),
        ]);
        let resumed = runtime
            .handle_incoming(
                connection_id,
                serde_json::json!({
                    "id": 31,
                    "method": "turn/resume",
                    "params": {
                        "sessionId": session_id,
                        "expectedTurnId": started.turn.id,
                        "recoveryRevision": recovery["revision"],
                        "idempotencyKey": "resume-memory-probe"
                    }
                }),
            )
            .await
            .context("turn/resume response")?;
        anyhow::ensure!(
            resumed.get("result").is_some(),
            "turn/resume failed: {resumed}"
        );
        support::wait_for_parent_turn_completed(&mut notifications, session_id).await?;

        let requests = provider.requests();
        let final_request = requests.last().context("resumed model request")?;
        let result =
            memory_forget_runtime_support::tool_result(final_request, "remember-after-recovery")
                .context("resumed memory_remember tool result")?;
        let remembered: MemoryEntry = serde_json::from_str(result)
            .with_context(|| format!("resumed memory_remember must succeed, got: {result}"))?;
        assert_memory_evidence(
            data_root.path(),
            &remembered.entry_id.to_string(),
            &session_id.to_string(),
            &started.turn.id.to_string(),
            original_user_items[0].id.as_str(),
        )?;
        let searched: MemorySearchResult = serde_json::from_str(
            memory_forget_runtime_support::tool_result(final_request, "search-after-recovery")
                .context("resumed memory_search result")?,
        )?;
        assert_eq!(
            searched,
            Page {
                data: vec![MemorySearchEntry {
                    entry_id: forget_target.entry_id.clone(),
                    scope: forget_target.scope,
                    kind: forget_target.kind,
                    state: MemoryState::Active,
                    summary: forget_target.body.clone(),
                }],
                next_cursor: None,
            }
        );
        let forgotten: MemoryForgetResult = serde_json::from_str(
            memory_forget_runtime_support::tool_result(final_request, "forget-after-recovery")
                .context("resumed memory_forget result")?,
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
                candidates: Vec::new(),
            }
        );
        let listed = runtime
            .handle_incoming(
                connection_id,
                serde_json::json!({
                    "id": 32,
                    "method": "memory/list",
                    "params": { "scope": "user", "state": "active" }
                }),
            )
            .await
            .context("memory/list response")?;
        let listed: devo_server::SuccessResponse<Page<MemoryEntry>> =
            serde_json::from_value(listed)?;
        assert_eq!(
            listed.result,
            Page {
                data: vec![remembered],
                next_cursor: None
            }
        );
        assert_eq!(
            user_items(&runtime, connection_id, session_id).await?,
            original_user_items
        );
        runtime.shutdown().await;
    }
    Ok(())
}

async fn user_items(
    runtime: &Arc<devo_server::ServerRuntime>,
    connection_id: u64,
    session_id: devo_protocol::SessionId,
) -> Result<Vec<ItemEnvelope>> {
    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 33,
                "method": "session/items/list",
                "params": { "sessionId": session_id }
            }),
        )
        .await
        .context("session/items/list response")?;
    let page: Page<ItemEnvelope> = serde_json::from_value(response["result"].clone())?;
    Ok(page
        .data
        .into_iter()
        .filter(|item| matches!(item.item, Item::UserMessage { .. }))
        .collect())
}

fn assert_memory_evidence(
    data_root: &std::path::Path,
    entry_id: &str,
    session_id: &str,
    turn_id: &str,
    user_item_id: &str,
) -> Result<()> {
    let connection = rusqlite::Connection::open(data_root.join("memory").join("memory.sqlite3"))?;
    let mut statement = connection.prepare(
        "SELECT session_id, turn_id, source_user_item_id FROM memory_evidence WHERE entry_id = ?1",
    )?;
    let evidence = statement
        .query_map([entry_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    // IDs come from the original Native turn/history, never from the write result.
    // Whole-vector equality also rejects duplicate or substituted evidence.
    assert_eq!(
        evidence,
        vec![(
            session_id.to_string(),
            Some(turn_id.to_string()),
            Some(user_item_id.to_string())
        )]
    );
    Ok(())
}
