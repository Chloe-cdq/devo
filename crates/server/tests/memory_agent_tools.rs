use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use devo_core::tools::{AgentToolCoordinator, MemoryToolInvocation};
use devo_protocol::native::rpc_memory::MemorySearchParams;
use pretty_assertions::assert_eq;

#[path = "support/memory_recall_gate.rs"]
mod gate;
#[path = "support/memory_forget_runtime.rs"]
#[allow(dead_code)]
mod memory_support;
#[path = "support/subagent_lifecycle.rs"]
#[allow(dead_code)]
mod support;

/// Trace: L2-DES-MEM-001 Rev 4 Built-in Agent Tools
/// Verifies: stable-ID reads return only bounded entry data and summarized provenance.
#[tokio::test]
async fn root_reads_safe_entry_by_stable_id() -> Result<()> {
    let data = memory_support::configured_data_root()?;
    let provider = Arc::new(support::ScriptedProvider::new([]));
    let runtime = support::build_runtime_with_workspace_config(data.path(), provider.clone())?;
    let (connection, mut notifications, session) =
        memory_support::start_subscribed_session(&runtime, data.path(), /*request_id*/ 1).await?;
    let entry =
        memory_support::remember(&runtime, connection, /*request_id*/ 2, "Use tabs").await?;
    provider.push_scripts([
        memory_support::tool_call_script(
            "read",
            "memory_read",
            serde_json::json!({"entry_id": entry.entry_id}),
        ),
        support::ScriptedProvider::completed("Read."),
    ]);
    memory_support::run_turn(
        &runtime,
        connection,
        session,
        &mut notifications,
        "Read that entry",
    )
    .await?;
    let requests = provider.requests();
    let result = memory_support::tool_result(&requests[1], "read").context("read result")?;
    let result = serde_json::from_str::<serde_json::Value>(result)
        .unwrap_or_else(|_| serde_json::json!({"error": result}));
    assert_eq!(
        result,
        serde_json::json!({
            "entryId": entry.entry_id, "scope": "user", "kind": "fact", "state": "active",
            "body": "Use tabs", "sourceSummary": "Explicit user memory (1 source)"
        })
    );
    runtime.shutdown().await;
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-6, Built-in Agent Tools
/// Verifies: delegated agents inherit the exact active parent snapshot even with no history,
/// after a store change and recall disable, and cannot bypass the search coordinator guard.
#[tokio::test]
async fn subagent_inherits_parent_snapshot_and_cannot_search_independently() -> Result<()> {
    for fork_turns in ["none", "all"] {
        let data = memory_support::configured_data_root()?;
        let (requests_tx, mut requests_rx) = tokio::sync::mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Semaphore::new(/*permits*/ 0));
        let provider = Arc::new(gate::GatedProvider {
            inner: support::ScriptedProvider::new([
                support::ScriptedProvider::completed("parent"),
                support::ScriptedProvider::completed("child"),
                support::ScriptedProvider::completed("follow-up"),
            ]),
            requests: requests_tx,
            release: release.clone(),
        });
        let runtime = support::build_runtime_with_workspace_config(data.path(), provider)?;
        let (connection, mut notifications, parent) =
            memory_support::start_subscribed_session(&runtime, data.path(), /*request_id*/ 10)
                .await?;
        memory_support::remember(&runtime, connection, /*request_id*/ 11, "Use tabs").await?;
        support::start_turn_with_approval_policy(
            &runtime,
            connection,
            parent,
            "Use tabs",
            Some("never"),
        )
        .await?;
        let first = tokio::time::timeout(Duration::from_secs(/*secs*/ 5), requests_rx.recv())
            .await?
            .context("parent request")?;
        let original = support::message_texts(&first)
            .into_iter()
            .find(|text| text.contains("<advisory_memory>"))
            .context("parent snapshot")?;
        memory_support::remember(
            &runtime,
            connection,
            /*request_id*/ 12,
            "Use new tabs advice",
        )
        .await?;
        let patch = runtime
        .handle_incoming(
            connection,
            serde_json::json!({
                "id": 13, "method": "session/metadata/update", "params": {
                    "sessionId": parent, "expectedVersion": 0, "settings": {"memoryRecall": "off"}
                }
            }),
        )
        .await
        .context("settings response")?;
        anyhow::ensure!(patch.get("result").is_some(), "settings failed: {patch}");
        let child =
            support::spawn_child_with(&runtime, connection, parent, "Use tabs", Some(fork_turns))
                .await?;
        let request = tokio::time::timeout(Duration::from_secs(/*secs*/ 5), requests_rx.recv())
            .await?
            .context("child request")?;
        let inherited = support::message_texts(&request)
            .into_iter()
            .find(|text| text.contains("<advisory_memory>"));
        assert_eq!(inherited, Some(original.clone()));
        assert!(
            request
                .tools
                .iter()
                .flatten()
                .all(|tool| !tool.name.starts_with("memory_"))
        );
        let item = support::wait_for_session_notification(
            &mut notifications,
            "item/completed",
            child.child_session_id,
        )
        .await?;
        let invocation = MemoryToolInvocation {
            session_id: child.child_session_id,
            turn_id: item["params"]["item"]["turnId"]
                .as_str()
                .context("child turn")?
                .try_into()?,
            user_item_id: item["params"]["item"]["id"]
                .as_str()
                .context("child user item")?
                .into(),
        };
        let error = runtime
            .clone()
            .memory_search(
                invocation,
                MemorySearchParams {
                    query: "tabs".into(),
                    scope: None,
                    kind: None,
                    state: None,
                },
            )
            .await
            .expect_err("delegated search must be denied");
        assert_eq!(
            error.to_string(),
            "denied: sub-agents cannot read or mutate user memory"
        );
        let patch = runtime
            .handle_incoming(
                connection,
                serde_json::json!({
                    "id": 14, "method": "session/metadata/update", "params": {
                        "sessionId": child.child_session_id, "expectedVersion": 0,
                        "settings": {"memoryRecall": "on", "memoryContribution": "on"}
                    }
                }),
            )
            .await
            .context("child settings response")?;
        assert!(patch.get("error").is_some());
        release.add_permits(2);
        support::wait_for_session_notification(
            &mut notifications,
            "turn/completed",
            child.child_session_id,
        )
        .await?;
        support::request_agent_send_message(
            &runtime,
            connection,
            parent,
            child.child_session_id,
            "Continue with the same context",
        )
        .await?;
        let followup = tokio::time::timeout(Duration::from_secs(/*secs*/ 5), requests_rx.recv())
            .await?
            .context("follow-up request")?;
        assert_eq!(
            support::message_texts(&followup)
                .into_iter()
                .find(|text| text.contains("<advisory_memory>")),
            Some(original)
        );
        release.add_permits(1);
        runtime.shutdown().await;
    }
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 Built-in Agent Tools
/// Verifies: query input is bounded at the runtime seam, including direct callers.
#[tokio::test]
async fn runtime_search_rejects_oversized_query() -> Result<()> {
    use devo_server::memory::{MemoryCommand, MemoryRuntime, SearchMemoryRequest};
    let data = tempfile::tempdir()?;
    let runtime = MemoryRuntime::open(
        data.path().join("memory"),
        devo_core::MemoryConfig {
            enabled: true,
            ..Default::default()
        },
    )?;
    let result = runtime
        .execute_command(MemoryCommand::Search(SearchMemoryRequest {
            query: "a".repeat(1025),
            scope: Default::default(),
            kind: None,
            state: None,
            workspace_root: data.path().to_path_buf(),
        }))
        .await;
    assert!(matches!(
        result,
        Err(devo_server::memory::MemoryError::InvalidRequest(_))
    ));
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 Privacy and Authority
/// Verifies: corrupt secret-bearing stored content never reaches search projections.
#[tokio::test]
async fn search_omits_secret_bearing_stored_entries() -> Result<()> {
    use devo_server::memory::{
        MemoryCommand, MemoryCommandResult, MemoryRuntime, SearchMemoryRequest,
    };
    let data = memory_support::configured_data_root()?;
    let provider = Arc::new(support::ScriptedProvider::new([]));
    let runtime = support::build_runtime_with_workspace_config(data.path(), provider)?;
    let (connection, _, _) =
        memory_support::start_subscribed_session(&runtime, data.path(), /*request_id*/ 20).await?;
    let entry =
        memory_support::remember(&runtime, connection, /*request_id*/ 21, "Use tabs").await?;
    let db = rusqlite::Connection::open(data.path().join("memory/memory.sqlite3"))?;
    db.execute(
        "UPDATE memory_entries SET body = 'tabs api_key=private-value' WHERE entry_id = ?1",
        [entry.entry_id.as_str()],
    )?;
    let memory = MemoryRuntime::open(
        data.path().join("memory"),
        devo_core::MemoryConfig {
            enabled: true,
            ..Default::default()
        },
    )?;
    let result = memory
        .execute_command(MemoryCommand::Search(SearchMemoryRequest {
            query: "tabs".into(),
            scope: Default::default(),
            kind: None,
            state: None,
            workspace_root: data.path().to_path_buf(),
        }))
        .await?;
    assert_eq!(
        result,
        MemoryCommandResult::Search(devo_protocol::native::page::Page {
            data: vec![],
            next_cursor: None,
        })
    );
    runtime.shutdown().await;
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 Session Settings, Built-in Agent Tools
/// Verifies: an ephemeral delegated session cannot change recall or contribution settings.
#[tokio::test]
async fn ephemeral_subagent_cannot_change_memory_settings() -> Result<()> {
    let data = memory_support::configured_data_root()?;
    let runtime = support::build_runtime_with_workspace_config(
        data.path(),
        Arc::new(support::ScriptedProvider::pending()),
    )?;
    let (connection, _, parent) =
        memory_support::start_subscribed_session(&runtime, data.path(), /*request_id*/ 30).await?;
    let child = runtime
        .clone()
        .spawn_agent(devo_protocol::SpawnAgentParams {
            session_id: parent,
            message: "Review".into(),
            fork_turns: Some("none".into()),
            max_turns: None,
            tool_policy: devo_protocol::AgentToolPolicy::Inherit,
            ephemeral: true,
        })
        .await?;
    let response = runtime
        .handle_incoming(
            connection,
            serde_json::json!({
                "id": 31, "method": "session/metadata/update", "params": {
                    "sessionId": child.child_session_id, "expectedVersion": 0,
                    "settings": {"memoryRecall": "on", "memoryContribution": "on"}
                }
            }),
        )
        .await
        .context("metadata response")?;
    assert!(
        response.get("error").is_some(),
        "delegated settings must be rejected: {response}"
    );
    runtime.shutdown().await;
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 Built-in Agent Tools
/// Verifies: delegated current-user bindings do not authorize either independent read surface.
#[tokio::test]
async fn coordinator_rejects_subagent_read_and_search() -> Result<()> {
    let data = memory_support::configured_data_root()?;
    let runtime = support::build_runtime_with_workspace_config(
        data.path(),
        Arc::new(support::ScriptedProvider::pending()),
    )?;
    let (connection, mut notifications, parent) =
        memory_support::start_subscribed_session(&runtime, data.path(), /*request_id*/ 40).await?;
    let entry =
        memory_support::remember(&runtime, connection, /*request_id*/ 41, "Use tabs").await?;
    let child = support::spawn_child(&runtime, connection, parent).await?;
    let item = support::wait_for_session_notification(
        &mut notifications,
        "item/completed",
        child.child_session_id,
    )
    .await?;
    let invocation = MemoryToolInvocation {
        session_id: child.child_session_id,
        turn_id: item["params"]["item"]["turnId"]
            .as_str()
            .context("child turn")?
            .try_into()?,
        user_item_id: item["params"]["item"]["id"]
            .as_str()
            .context("child user item")?
            .into(),
    };
    let read = runtime
        .clone()
        .memory_read(invocation.clone(), entry.entry_id)
        .await
        .expect_err("subagent read denied");
    let search = runtime
        .clone()
        .memory_search(
            invocation,
            MemorySearchParams {
                query: "tabs".into(),
                scope: None,
                kind: None,
                state: None,
            },
        )
        .await
        .expect_err("subagent search denied");
    assert_eq!(
        (read.to_string(), search.to_string()),
        (
            "denied: sub-agents cannot read or mutate user memory".to_string(),
            "denied: sub-agents cannot read or mutate user memory".to_string(),
        )
    );
    runtime.shutdown().await;
    Ok(())
}
