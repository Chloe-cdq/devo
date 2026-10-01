use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use devo_protocol::native::item::{Item, ItemEnvelope};
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_session::RestorePlan;
use pretty_assertions::assert_eq;

#[path = "support/memory_recall_gate.rs"]
mod gate;
#[path = "support/memory_forget_runtime.rs"]
#[allow(dead_code)]
mod memory_support;
#[path = "support/subagent_lifecycle.rs"]
#[allow(dead_code)]
mod support;

enum HistoryRewrite {
    MessageEdit,
    Rollback,
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-6
/// Verifies: an edited child prompt still receives the original parent memory snapshot.
#[tokio::test]
async fn child_message_edit_preserves_inherited_memory() -> Result<()> {
    assert_inherited_memory_after_rewrite(HistoryRewrite::MessageEdit).await
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-6
/// Verifies: a child follow-up after rollback still receives the original parent snapshot.
#[tokio::test]
async fn child_rollback_preserves_inherited_memory() -> Result<()> {
    assert_inherited_memory_after_rewrite(HistoryRewrite::Rollback).await
}

async fn assert_inherited_memory_after_rewrite(rewrite: HistoryRewrite) -> Result<()> {
    let data = memory_support::configured_data_root()?;
    let (requests_tx, mut requests_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(/*permits*/ 0));
    let provider = Arc::new(gate::GatedProvider {
        inner: support::ScriptedProvider::new(
            (0..4).map(|_| support::ScriptedProvider::completed("done")),
        ),
        requests: requests_tx,
        release: release.clone(),
    });
    let runtime = support::build_runtime_with_workspace_config(data.path(), provider)?;
    let (connection, mut notifications, parent) =
        memory_support::start_subscribed_session(&runtime, data.path(), /*request_id*/ 1).await?;
    memory_support::remember(&runtime, connection, /*request_id*/ 2, "Use tabs").await?;
    support::start_turn_with_approval_policy(
        &runtime,
        connection,
        parent,
        "Use tabs",
        Some("never"),
    )
    .await?;
    let request = tokio::time::timeout(Duration::from_secs(/*secs*/ 5), requests_rx.recv())
        .await?
        .context("parent request")?;
    let original = support::message_texts(&request)
        .into_iter()
        .find(|text| text.contains("<advisory_memory>"))
        .context("parent snapshot")?;
    memory_support::remember(
        &runtime,
        connection,
        /*request_id*/ 3,
        "Use new tabs advice",
    )
    .await?;
    let patch = runtime
        .handle_incoming(
            connection,
            serde_json::json!({
                "id": 4, "method": "session/metadata/update", "params": {
                    "sessionId": parent, "expectedVersion": 0, "settings": {"memoryRecall": "off"}
                }
            }),
        )
        .await
        .context("settings response")?;
    anyhow::ensure!(patch.get("result").is_some(), "settings failed: {patch}");
    let child = support::spawn_child_with(&runtime, connection, parent, "Use tabs", Some("none"))
        .await?
        .child_session_id;
    let request = tokio::time::timeout(Duration::from_secs(/*secs*/ 5), requests_rx.recv())
        .await?
        .context("child request")?;
    assert_eq!(
        support::message_texts(&request)
            .into_iter()
            .find(|text| text.contains("<advisory_memory>")),
        Some(original.clone())
    );
    release.add_permits(2);
    support::wait_for_session_notification(&mut notifications, "turn/completed", child).await?;

    match rewrite {
        HistoryRewrite::MessageEdit => {
            let items = runtime
                .handle_incoming(
                    connection,
                    serde_json::json!({
                        "id": 5, "method": "session/items/list", "params": {"sessionId": child}
                    }),
                )
                .await
                .context("child items")?;
            let page: Page<ItemEnvelope> = serde_json::from_value(items["result"].clone())?;
            let item = page
                .data
                .iter()
                .find(|item| matches!(item.item, Item::UserMessage { .. }))
                .context("child user item")?;
            let edited = runtime
                .handle_incoming(
                    connection,
                    serde_json::json!({
                        "id": 6, "method": "session/message/edit", "params": {
                            "sessionId": child, "itemId": item.id, "expectedRevision": item.revision,
                            "content": [{"type": "text", "text": "Use tabs after editing"}],
                            "workspaceRestore": "skip", "idempotencyKey": "child-memory-edit"
                        }
                    }),
                )
                .await
                .context("child edit")?;
            anyhow::ensure!(edited.get("result").is_some(), "edit failed: {edited}");
        }
        HistoryRewrite::Rollback => {
            support::request_agent_send_message(
                &runtime,
                connection,
                parent,
                child,
                "Use tabs again",
            )
            .await?;
            tokio::time::timeout(Duration::from_secs(/*secs*/ 5), requests_rx.recv())
                .await?
                .context("second child request")?;
            release.add_permits(1);
            support::wait_for_session_notification(&mut notifications, "turn/completed", child)
                .await?;
            let preview = runtime
                .handle_incoming(
                    connection,
                    serde_json::json!({
                        "id": 5, "method": "session/rollback/preview", "params": {
                            "sessionId": child, "userTurnIndex": 1, "mode": "beforeUserTurn"
                        }
                    }),
                )
                .await
                .context("child rollback preview")?;
            let plan: RestorePlan = serde_json::from_value(preview["result"].clone())?;
            let commit = runtime
                .handle_incoming(
                    connection,
                    serde_json::json!({
                        "id": 6, "method": "session/rollback/commit", "params": {
                            "restorePlanId": plan.restore_plan_id,
                            "expectedWorkspaceVersion": plan.workspace_version
                        }
                    }),
                )
                .await
                .context("child rollback commit")?;
            anyhow::ensure!(commit.get("result").is_some(), "rollback failed: {commit}");
            support::request_agent_send_message(
                &runtime,
                connection,
                parent,
                child,
                "Use tabs after rollback",
            )
            .await?;
        }
    }
    let request = tokio::time::timeout(Duration::from_secs(/*secs*/ 5), requests_rx.recv())
        .await?
        .context("child request after history rewrite")?;
    let inherited = support::message_texts(&request)
        .into_iter()
        .find(|text| text.contains("<advisory_memory>"));
    assert!(
        request
            .tools
            .iter()
            .flatten()
            .all(|tool| !tool.name.starts_with("memory_"))
    );
    release.add_permits(1);
    support::wait_for_session_notification(&mut notifications, "turn/completed", child).await?;
    runtime.shutdown().await;
    assert_eq!(inherited, Some(original));
    Ok(())
}
