use std::sync::Arc;

use anyhow::{Context, Result};
use devo_protocol::SessionId;
use pretty_assertions::assert_eq;

#[path = "support/memory_forget_runtime.rs"]
#[allow(dead_code)]
mod memory_support;
#[path = "support/subagent_lifecycle.rs"]
#[allow(dead_code)]
mod support;

async fn automation_session(
    runtime: &Arc<devo_server::ServerRuntime>,
    root: &std::path::Path,
) -> Result<(
    u64,
    tokio::sync::mpsc::Receiver<serde_json::Value>,
    SessionId,
)> {
    let (connection, notifications) = support::initialize_connection(runtime).await?;
    let response = runtime
        .handle_incoming(
            connection,
            serde_json::json!({
                "id": 1, "method": "session/new", "params": {
                    "cwd": root, "idempotencyKey": "automation", "source": "automation"
                }
            }),
        )
        .await
        .context("session/new")?;
    let session = serde_json::from_value(response["result"]["session"]["id"].clone())?;
    let response = runtime.handle_incoming(connection, serde_json::json!({
        "id": 2, "method": "subscription/create", "params": {
            "selectors": [{"kind": "session", "sessionId": session}], "includeSnapshot": false
        }
    })).await.context("subscription/create")?;
    anyhow::ensure!(
        response.get("result").is_some(),
        "subscription failed: {response}"
    );
    Ok((connection, notifications, session))
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-1/DD-2
/// Verifies: automation identity survives settings, fork, and cold resume.
#[tokio::test]
async fn automation_identity_survives_settings_fork_and_restart() -> Result<()> {
    let data = memory_support::configured_data_root()?;
    let provider = Arc::new(support::ScriptedProvider::new([]));
    let runtime = support::build_runtime_with_workspace_config(data.path(), provider.clone())?;
    let (connection, _, session) = automation_session(&runtime, data.path()).await?;
    let updated = runtime
        .handle_incoming(
            connection,
            serde_json::json!({
                "id": 3, "method": "session/metadata/update", "params": {
                    "sessionId": session, "expectedVersion": 0,
                    "settings": {"memoryRecall": "on", "memoryContribution": "on"}
                }
            }),
        )
        .await
        .context("metadata update")?;
    assert_eq!(
        updated["result"]["session"]["source"],
        serde_json::json!("automation")
    );
    let fork = runtime
        .handle_incoming(
            connection,
            serde_json::json!({
                "id": 4, "method": "session/fork", "params": {"sessionId": session}
            }),
        )
        .await
        .context("fork")?;
    assert_eq!(
        fork["result"]["session"]["source"],
        serde_json::json!("automation")
    );
    let fork_id = fork["result"]["session"]["id"].clone();
    runtime.shutdown().await;
    drop(runtime);
    let runtime = support::build_runtime_with_workspace_config(data.path(), provider)?;
    let (connection, _) = support::initialize_connection(&runtime).await?;
    for id in [serde_json::json!(session), fork_id] {
        let resumed = runtime
            .handle_incoming(
                connection,
                serde_json::json!({
                    "id": 5, "method": "session/resume", "params": {"sessionId": id}
                }),
            )
            .await
            .context("resume")?;
        assert_eq!(
            resumed["result"]["session"]["source"],
            serde_json::json!("automation")
        );
    }
    runtime.shutdown().await;
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-1/DD-8
/// Verifies: an automation prompt cannot promote its private memory through the agent tool.
#[tokio::test]
async fn automation_cannot_promote_private_memory_with_remember_tool() -> Result<()> {
    let data = memory_support::configured_data_root()?;
    let provider = Arc::new(support::ScriptedProvider::new([
        memory_support::tool_call_script(
            "private-memory",
            "memory_remember",
            serde_json::json!({"text": "Private automation fact", "scope": "user"}),
        ),
        support::ScriptedProvider::completed("ok"),
    ]));
    let runtime = support::build_runtime_with_workspace_config(data.path(), provider.clone())?;
    let (connection, mut notifications, session) =
        automation_session(&runtime, data.path()).await?;
    memory_support::run_turn(
        &runtime,
        connection,
        session,
        &mut notifications,
        "Remember this for future sessions: Private automation fact",
    )
    .await?;
    let requests = provider.requests();
    let result =
        memory_support::tool_result(&requests[1], "private-memory").context("tool result")?;
    assert!(
        result.contains("automation"),
        "automation write must be rejected: {result}"
    );
    let db = rusqlite::Connection::open(data.path().join("memory").join("memory.sqlite3"))?;
    for table in [
        "memory_entries",
        "memory_evidence",
        "memory_candidates",
        "memory_jobs",
    ] {
        let count: i64 = db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })?;
        assert_eq!(count, 0, "automation must not populate {table}");
    }
    runtime.shutdown().await;
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-1/DD-2
/// Verifies: contribution On never admits an automation transcript for extraction.
#[tokio::test]
async fn automation_sources_are_ineligible_even_with_contribution_on() -> Result<()> {
    use devo_protocol::native::session::{MemorySetting, SessionSource};
    use devo_server::memory::{EnqueueOutcome, MemoryRuntime, SessionMemorySource};
    let data = tempfile::TempDir::new()?;
    let memory = MemoryRuntime::open(
        data.path().join("memory"),
        devo_core::MemoryConfig {
            enabled: true,
            default_contribution: MemorySetting::On,
            ..Default::default()
        },
    )?;
    for session_contribution in [
        MemorySetting::Inherit,
        MemorySetting::On,
        MemorySetting::Off,
    ] {
        assert_eq!(
            memory
                .enqueue_source(SessionMemorySource {
                    source: SessionSource::Automation,
                    session_contribution,
                })
                .await?,
            EnqueueOutcome { accepted: false }
        );
    }
    assert_eq!(
        memory
            .enqueue_source(SessionMemorySource {
                source: SessionSource::Interactive,
                session_contribution: MemorySetting::On,
            })
            .await?,
        EnqueueOutcome { accepted: true }
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-1/DD-6
/// Verifies: enabled automation recall consumes the same bounded snapshot without writing private data.
#[tokio::test]
async fn automation_consumes_bounded_recall_without_merging_private_namespace() -> Result<()> {
    let data = memory_support::configured_data_root()?;
    let provider = Arc::new(support::ScriptedProvider::new([
        memory_support::tool_call_script(
            "read-memory",
            "memory_search",
            serde_json::json!({"query": "tabs"}),
        ),
        support::ScriptedProvider::completed("ok"),
    ]));
    let runtime = support::build_runtime_with_workspace_config(data.path(), provider.clone())?;
    let (interactive, _, _) =
        memory_support::start_subscribed_session(&runtime, data.path(), /*request_id*/ 10).await?;
    for index in 0..20 {
        memory_support::remember(
            &runtime,
            interactive,
            11 + index,
            &format!("tabs convention {index}"),
        )
        .await?;
    }
    let project = runtime.handle_incoming(interactive, serde_json::json!({
        "id": 35, "method": "memory/remember", "params": {"text": "tabs project convention", "scope": "project"}
    })).await.context("project memory")?;
    anyhow::ensure!(
        project.get("result").is_some(),
        "project remember failed: {project}"
    );
    let private_path = data
        .path()
        .join("automations")
        .join("daily")
        .join("memory.md");
    std::fs::create_dir_all(private_path.parent().context("private parent")?)?;
    let private = "Private automation memory: review watermark 2030-05-10";
    std::fs::write(&private_path, private)?;
    let (connection, mut notifications, session) =
        automation_session(&runtime, data.path()).await?;
    let db = rusqlite::Connection::open(data.path().join("memory").join("memory.sqlite3"))?;
    let before: Vec<(String, String)> = db
        .prepare("SELECT entry_id, body FROM memory_entries ORDER BY entry_id")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<std::result::Result<_, _>>()?;
    memory_support::run_turn(
        &runtime,
        connection,
        session,
        &mut notifications,
        &format!("Use tabs.\n<automation_run_memory>\n{private}\n</automation_run_memory>"),
    )
    .await?;
    let requests = provider.requests();
    let texts = support::message_texts(&requests[0]);
    let general = texts
        .iter()
        .find(|text| text.starts_with("<advisory_memory>"))
        .context("general recall")?;
    assert!(
        general.contains("General Persistent Memory"),
        "general namespace must be labeled"
    );
    assert!(general.contains("tabs project convention"));
    assert_eq!(requests.len(), 2);
    assert_eq!(
        support::message_texts(&requests[1])
            .into_iter()
            .find(|text| text.starts_with("<advisory_memory>")),
        Some(general.clone())
    );
    assert!(general.len().div_ceil(4) <= 2000);
    assert!(!general.contains(private));
    assert!(texts.iter().any(|text| text.contains(private)));
    let items = runtime
        .handle_incoming(
            connection,
            serde_json::json!({
                "id": 40, "method": "session/items/list", "params": {"sessionId": session}
            }),
        )
        .await
        .context("items")?;
    let recalls: Vec<_> = items["result"]["data"]
        .as_array()
        .context("items page")?
        .iter()
        .filter(|item| item["item"]["type"] == "memoryRecall")
        .collect();
    assert_eq!(recalls.len(), 1);
    assert_eq!(
        recalls[0]["item"]["entries"]
            .as_array()
            .context("entries")?
            .len(),
        12
    );
    let after: Vec<(String, String)> = db
        .prepare("SELECT entry_id, body FROM memory_entries ORDER BY entry_id")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(after, before);
    assert_eq!(std::fs::read_to_string(private_path)?, private);
    for table in ["memory_candidates", "memory_jobs"] {
        let count: i64 = db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })?;
        assert_eq!(count, 0);
    }
    runtime.shutdown().await;
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-2/DD-6
/// Verifies: session Off and global disable suppress automation general memory context.
#[tokio::test]
async fn automation_recall_respects_session_off_and_global_disable() -> Result<()> {
    for (config, recall) in [
        ("[memory]\nenabled = true\n", "off"),
        ("[memory]\nenabled = false\n", "on"),
    ] {
        let data = memory_support::configured_data_root()?;
        let memory = devo_server::memory::MemoryRuntime::open(
            data.path().join("memory"),
            devo_core::MemoryConfig {
                enabled: true,
                ..Default::default()
            },
        )?;
        memory
            .execute_command(devo_server::memory::MemoryCommand::Remember(
                devo_server::memory::MemoryRememberRequest {
                    text: "Use tabs".into(),
                    scope: devo_protocol::native::rpc_memory::MemoryScope::User,
                    kind: None,
                    source: devo_server::memory::MemorySourceContext {
                        session_id: SessionId::new(),
                        turn_id: None,
                        user_item_id: None,
                        workspace_root: data.path().to_path_buf(),
                    },
                },
            ))
            .await?;
        drop(memory);
        std::fs::write(data.path().join(".devo").join("config.toml"), config)?;
        let provider = Arc::new(support::ScriptedProvider::new([
            support::ScriptedProvider::completed("ok"),
        ]));
        let runtime = support::build_runtime_with_workspace_config(data.path(), provider.clone())?;
        let (connection, mut notifications, session) =
            automation_session(&runtime, data.path()).await?;
        let patch = runtime.handle_incoming(connection, serde_json::json!({
            "id": 3, "method": "session/metadata/update", "params": {
                "sessionId": session, "expectedVersion": 0, "settings": {"memoryRecall": recall}
            }
        })).await.context("recall patch")?;
        anyhow::ensure!(patch.get("result").is_some(), "patch failed: {patch}");
        memory_support::run_turn(
            &runtime,
            connection,
            session,
            &mut notifications,
            "Use tabs",
        )
        .await?;
        assert!(
            support::message_texts(&provider.requests()[0])
                .iter()
                .all(|text| !text.starts_with("<advisory_memory>"))
        );
        runtime.shutdown().await;
    }
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-6
/// Verifies: recall storage failure leaves the automation running with its private context.
#[tokio::test]
async fn automation_recall_failure_does_not_prevent_run() -> Result<()> {
    let data = memory_support::configured_data_root()?;
    let provider = Arc::new(support::ScriptedProvider::new([
        support::ScriptedProvider::completed("ok"),
    ]));
    let runtime = support::build_runtime_with_workspace_config(data.path(), provider.clone())?;
    let (connection, mut notifications, session) =
        automation_session(&runtime, data.path()).await?;
    let db = rusqlite::Connection::open(data.path().join("memory").join("memory.sqlite3"))?;
    db.execute_batch("DROP TABLE memory_entries_fts;")?;
    memory_support::run_turn(
        &runtime,
        connection,
        session,
        &mut notifications,
        "Use tabs. Automation Run Memory: private last-run watermark",
    )
    .await?;
    let texts = support::message_texts(&provider.requests()[0]);
    assert!(
        texts
            .iter()
            .all(|text| !text.starts_with("<advisory_memory>"))
    );
    assert!(
        texts
            .iter()
            .any(|text| text.contains("private last-run watermark"))
    );
    runtime.shutdown().await;
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-1/DD-8
/// Verifies: direct Native automation writes cannot bypass the agent tool boundary.
#[tokio::test]
async fn automation_native_mutations_are_rejected_for_both_scopes() -> Result<()> {
    let data = memory_support::configured_data_root()?;
    let runtime = support::build_runtime_with_workspace_config(
        data.path(),
        Arc::new(support::ScriptedProvider::new([])),
    )?;
    let (interactive, _, _) =
        memory_support::start_subscribed_session(&runtime, data.path(), /*request_id*/ 10).await?;
    let (connection, _, _) = automation_session(&runtime, data.path()).await?;
    for scope in ["user", "project"] {
        let seed = runtime.handle_incoming(interactive, serde_json::json!({
            "id": 11, "method": "memory/remember", "params": {"text": "Interactive fact", "scope": scope}
        })).await.context("seed memory")?;
        anyhow::ensure!(seed.get("result").is_some(), "seed failed: {seed}");
        let before = runtime
            .handle_incoming(
                interactive,
                serde_json::json!({
                    "id": 12, "method": "memory/list", "params": {"scope": scope}
                }),
            )
            .await
            .context("list before")?;
        for (method, params) in [
            (
                "memory/remember",
                serde_json::json!({"text": "Private automation fact", "scope": scope}),
            ),
            (
                "memory/forget",
                serde_json::json!({"entryId": seed["result"]["entryId"], "scope": scope}),
            ),
            (
                "memory/forget",
                serde_json::json!({"text": "Interactive fact", "scope": scope}),
            ),
        ] {
            let result = runtime
                .handle_incoming(
                    connection,
                    serde_json::json!({
                        "id": 13, "method": method, "params": params
                    }),
                )
                .await
                .context("mutation")?;
            assert!(
                result.get("error").is_some(),
                "automation must not mutate: {result}"
            );
        }
        let after = runtime
            .handle_incoming(
                interactive,
                serde_json::json!({
                    "id": 14, "method": "memory/list", "params": {"scope": scope}
                }),
            )
            .await
            .context("list after")?;
        assert_eq!(after["result"], before["result"]);
    }
    runtime.shutdown().await;
    Ok(())
}

#[path = "support/memory_recall_gate.rs"]
mod gate;

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-1/DD-8
/// Verifies: an idle automation selector does not block the active interactive source.
#[tokio::test]
async fn interactive_project_mutations_survive_idle_automation_selector() -> Result<()> {
    let data = memory_support::configured_data_root()?;
    let (requests, mut requests_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(/*permits*/ 0));
    let provider = Arc::new(gate::GatedProvider {
        inner: support::ScriptedProvider::new([support::ScriptedProvider::completed("ok")]),
        requests,
        release: release.clone(),
    });
    let runtime = support::build_runtime_with_workspace_config(data.path(), provider)?;
    let (connection, mut notifications, interactive) =
        memory_support::start_subscribed_session(&runtime, data.path(), /*request_id*/ 10).await?;
    let created = runtime
        .handle_incoming(
            connection,
            serde_json::json!({
                "id": 11, "method": "session/new", "params": {
                    "cwd": data.path(), "idempotencyKey": "idle-auto", "source": "automation"
                }
            }),
        )
        .await
        .context("create automation")?;
    let automation = created["result"]["session"]["id"].clone();
    runtime.handle_incoming(connection, serde_json::json!({
        "id": 12, "method": "subscription/create", "params": {
            "selectors": [{"kind": "session", "sessionId": automation}], "includeSnapshot": false
        }
    })).await.context("subscribe automation")?;
    support::start_turn_with_approval_policy(
        &runtime,
        connection,
        interactive,
        "Work",
        Some("never"),
    )
    .await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), requests_rx.recv())
        .await?
        .context("active request")?;
    let remembered = runtime
        .handle_incoming(
            connection,
            serde_json::json!({
                "id": 13, "method": "memory/remember", "params": {
                    "scope": "project", "text": "Use tabs"
                }
            }),
        )
        .await
        .context("remember")?;
    let forgotten = runtime
        .handle_incoming(
            connection,
            serde_json::json!({
                "id": 15, "method": "memory/forget", "params": {
                    "entryId": remembered["result"]["entryId"], "scope": "user"
                }
            }),
        )
        .await
        .context("forget Project entry despite User wire scope")?;
    release.add_permits(1);
    support::wait_for_parent_turn_completed(&mut notifications, interactive).await?;
    anyhow::ensure!(
        remembered.get("result").is_some(),
        "interactive mutation failed: {remembered}"
    );
    assert_eq!(
        remembered["result"]["provenance"],
        serde_json::json!([{ "sourceSessionId": interactive, "sourceTurnId": null, "sourceUserItemId": null }])
    );
    anyhow::ensure!(
        forgotten.get("result").is_some(),
        "interactive forget failed: {forgotten}"
    );
    let mut expected_entry = remembered["result"].clone();
    expected_entry["state"] = serde_json::json!("retired");
    expected_entry["updatedAt"] = forgotten["result"]["forgotten"]["updatedAt"].clone();
    assert_eq!(
        forgotten["result"],
        serde_json::json!({"forgotten": expected_entry, "candidates": []})
    );
    // With both selectors idle, a direct request has no trustworthy execution source.
    let idle = runtime.handle_incoming(connection, serde_json::json!({
        "id": 14, "method": "memory/remember", "params": {"scope": "project", "text": "idle fact"}
    })).await.context("idle remember")?;
    assert!(
        idle.get("error").is_some(),
        "mixed idle sources must fail closed: {idle}"
    );
    runtime.shutdown().await;
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-1/DD-2/DD-8
/// Verifies: a cold persistent automation child cannot mutate either General Memory scope.
#[tokio::test]
async fn automation_child_native_mutations_are_rejected_after_restart() -> Result<()> {
    let data = memory_support::configured_data_root()?;
    let provider = Arc::new(support::ScriptedProvider::pending());
    let runtime = support::build_runtime_with_workspace_config(data.path(), provider.clone())?;
    let (connection, _, parent) = automation_session(&runtime, data.path()).await?;
    let child = support::spawn_child_with(
        &runtime,
        connection,
        parent,
        "Automation Run Memory: private last-run watermark",
        Some("all"),
    )
    .await?;
    support::wait_for_stream_calls(&provider, /*expected*/ 1).await?;
    runtime.shutdown().await;
    drop(runtime);

    let runtime = support::build_runtime_with_workspace_config(
        data.path(),
        Arc::new(support::ScriptedProvider::new([])),
    )?;
    let (interactive, _, _) =
        memory_support::start_subscribed_session(&runtime, data.path(), /*request_id*/ 10).await?;
    let (connection, _) = support::initialize_connection(&runtime).await?;
    // Select the cold child without resuming it or loading a session actor.
    let subscribed = runtime
        .handle_incoming(
            connection,
            serde_json::json!({
                "id": 11, "method": "subscription/create", "params": {
                    "selectors": [{"kind": "session", "sessionId": child.child_session_id}],
                    "includeSnapshot": false
                }
            }),
        )
        .await
        .context("subscribe cold child")?;
    anyhow::ensure!(
        subscribed.get("result").is_some(),
        "subscription failed: {subscribed}"
    );

    for scope in ["user", "project"] {
        let seed = runtime.handle_incoming(interactive, serde_json::json!({
            "id": 12, "method": "memory/remember", "params": {"text": "Interactive fact", "scope": scope}
        })).await.context("seed memory")?;
        anyhow::ensure!(seed.get("result").is_some(), "seed failed: {seed}");
        let before = runtime
            .handle_incoming(
                interactive,
                serde_json::json!({
                    "id": 13, "method": "memory/list", "params": {"scope": scope}
                }),
            )
            .await
            .context("list before")?;
        anyhow::ensure!(before.get("result").is_some(), "list failed: {before}");
        let child_read = runtime
            .handle_incoming(
                connection,
                serde_json::json!({
                    "id": 14, "method": "memory/list", "params": {"scope": scope}
                }),
            )
            .await
            .context("child list")?;
        assert_eq!(child_read["result"], before["result"]);

        for (method, params) in [
            (
                "memory/remember",
                serde_json::json!({"text": "Private last-run watermark", "scope": scope}),
            ),
            (
                "memory/forget",
                serde_json::json!({"entryId": seed["result"]["entryId"], "scope": scope}),
            ),
            (
                "memory/forget",
                serde_json::json!({"text": "Interactive fact", "scope": scope}),
            ),
        ] {
            let response = runtime
                .handle_incoming(
                    connection,
                    serde_json::json!({
                        "id": 15, "method": method, "params": params
                    }),
                )
                .await
                .context("child mutation")?;
            assert!(
                response["error"]["message"]
                    .as_str()
                    .is_some_and(|message| message.contains("automation")),
                "cold automation child must reject {scope} {method}: {response}"
            );
        }
        let after = runtime
            .handle_incoming(
                interactive,
                serde_json::json!({
                    "id": 16, "method": "memory/list", "params": {"scope": scope}
                }),
            )
            .await
            .context("list after")?;
        assert_eq!(after["result"], before["result"]);
    }
    runtime.shutdown().await;
    Ok(())
}
