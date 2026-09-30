//! Capture the complete server failure path, including spawned turn finalization.

use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use devo_provider::openai::OpenAIProvider;
use devo_provider::{ModelProviderSDK, ProviderHttpOptions};
use pretty_assertions::assert_eq;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[path = "../../core/tests/support/memory_log_privacy.rs"]
mod log_support;
#[path = "support/subagent_lifecycle.rs"]
#[allow(dead_code)]
mod support;

use log_support::{EagerHttpErrorProvider, log_subscriber, read_http_request, write_http_response};

const MEMORY: &str = "Use tabs";
const CONVERSATION: &str = "Private earlier conversation";

#[derive(Clone, Copy, Debug)]
enum FailurePath {
    StreamCreation,
    HttpStream,
    StreamPayload,
    Compaction,
}

async fn exercise_failure_path(path: FailurePath) -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (finished_tx, mut finished_rx) = oneshot::channel::<()>();
    let gateway = tokio::spawn(async move {
        let mut requests = Vec::new();
        loop {
            let (mut socket, request) = tokio::select! {
                incoming = read_http_request(&listener) => incoming?,
                _ = &mut finished_rx => return Ok::<_, anyhow::Error>(requests),
            };
            let advisory = request["messages"]
                .as_array()
                .context("messages")?
                .iter()
                .filter_map(|message| message["content"].as_str())
                .find(|text| text.starts_with("<advisory_memory>"));
            let streaming = request["stream"] == true;
            let should_fail =
                advisory.is_some() && (!matches!(path, FailurePath::Compaction) || !streaming);
            if should_fail {
                let body = serde_json::json!({"error": {
                    "message": format!("Rejected context: {}\n{CONVERSATION}", advisory.context("recall")?),
                    "type": "invalid_request_error", "code": 400
                }});
                if matches!(path, FailurePath::StreamPayload) {
                    write_http_response(
                        &mut socket,
                        "200 OK",
                        "text/event-stream",
                        &format!("data: {body}\n\ndata: [DONE]\n\n"),
                    )
                    .await?;
                } else {
                    write_http_response(
                        &mut socket,
                        "400 Bad Request",
                        "application/json",
                        &body.to_string(),
                    )
                    .await?;
                }
            } else if streaming {
                let chunk = serde_json::json!({"id": "complete", "choices": [
                    {"index": 0, "delta": {"content": "done"}, "finish_reason": "stop"}
                ], "usage": {"prompt_tokens": 2_000_000, "completion_tokens": 1, "total_tokens": 2_000_001}});
                write_http_response(
                    &mut socket,
                    "200 OK",
                    "text/event-stream",
                    &format!("data: {chunk}\n\ndata: [DONE]\n\n"),
                )
                .await?;
            } else {
                let response = serde_json::json!({"id": "title", "choices": [
                    {"index": 0, "message": {"role": "assistant", "content": "title"}, "finish_reason": "stop"}
                ], "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}});
                write_http_response(
                    &mut socket,
                    "200 OK",
                    "application/json",
                    &response.to_string(),
                )
                .await?;
            }
            requests.push(request);
        }
    });
    let http_provider = OpenAIProvider::new(format!("http://{address}/v1")).with_http_options(
        ProviderHttpOptions::from_raw_with_no_proxy(
            /*proxy_url*/ None,
            Some("127.0.0.1".into()),
            /*headers*/ None,
        )?,
    )?;
    let provider: Arc<dyn ModelProviderSDK> = match path {
        FailurePath::StreamCreation => Arc::new(EagerHttpErrorProvider(http_provider)),
        FailurePath::HttpStream | FailurePath::StreamPayload | FailurePath::Compaction => {
            Arc::new(http_provider)
        }
    };
    let data = tempfile::tempdir()?;
    std::fs::create_dir_all(data.path().join(".devo"))?;
    std::fs::write(
        data.path().join(".devo").join("config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    let runtime = support::build_runtime_with_workspace_config(data.path(), provider)?;
    let (connection, mut notifications) = support::initialize_connection(&runtime).await?;
    let session = support::start_parent_session(&runtime, connection, data.path()).await?;
    for (method, params) in [
        (
            "subscription/create",
            serde_json::json!({"selectors": [{"kind": "session", "sessionId": session}], "includeSnapshot": false}),
        ),
        (
            "memory/remember",
            serde_json::json!({"text": MEMORY, "scope": "user"}),
        ),
    ] {
        let response = runtime
            .handle_incoming(
                connection,
                serde_json::json!({"id": 200, "method": method, "params": params}),
            )
            .await
            .context("Native response")?;
        anyhow::ensure!(
            response.get("result").is_some(),
            "{method} failed: {response}"
        );
    }
    if matches!(path, FailurePath::HttpStream) {
        let private_model = format!("{MEMORY} {CONVERSATION}");
        let response = runtime.handle_incoming(connection, serde_json::json!({
            "id": 205, "method": "provider/validate", "params": {
                "provider": {"id": "privacy", "name": "privacy", "enabled": true,
                    "wireApis": [devo_protocol::ProviderWireApi::OpenAIChatCompletions], "models": {}},
                "model": private_model,
            }
        })).await.context("provider validation response")?;
        assert_eq!(
            response["error"]["message"],
            serde_json::json!(format!(
                "model {private_model} is not present in provider directory"
            ))
        );
    }
    if matches!(path, FailurePath::Compaction) {
        support::start_turn_with_approval_policy(
            &runtime,
            connection,
            session,
            &format!("{}{CONVERSATION}", "x".repeat(/*n*/ 80_004)),
            Some("never"),
        )
        .await?;
        wait_notification(&mut notifications, "turn/completed", session).await?;
        wait_notification(&mut notifications, "session/statusChanged", session).await?;
    }
    let started = support::start_turn_with_approval_policy(
        &runtime,
        connection,
        session,
        &format!("{MEMORY}; {CONVERSATION}"),
        Some("never"),
    )
    .await?;
    let completed = wait_notification(&mut notifications, "turn/completed", session).await?;
    if matches!(path, FailurePath::Compaction) {
        assert_eq!(completed["params"]["turn"]["status"], "completed");
    } else {
        assert_eq!(completed["params"]["turn"]["status"], "failed");
        let message = completed["params"]["turn"]["error"]["message"]
            .as_str()
            .context("user error")?;
        assert!(message.contains(MEMORY));
        assert!(message.contains(CONVERSATION));
        let recovery =
            wait_notification(&mut notifications, "turn/recoveryUpdated", session).await?;
        let resumed = runtime
            .handle_incoming(
                connection,
                serde_json::json!({
                    "id": 202, "method": "turn/resume", "params": {
                        "sessionId": session, "expectedTurnId": started.turn.id,
                        "recoveryRevision": recovery["params"]["recovery"]["revision"],
                        "idempotencyKey": "privacy-recovery"
                    }
                }),
            )
            .await
            .context("resume response")?;
        anyhow::ensure!(resumed.get("result").is_some(), "resume failed: {resumed}");
        let completed = wait_notification(&mut notifications, "turn/completed", session).await?;
        assert_eq!(completed["params"]["turn"]["status"], "failed");
        let message = completed["params"]["turn"]["error"]["message"]
            .as_str()
            .context("recovered user error")?;
        assert!(message.contains(MEMORY));
        assert!(message.contains(CONVERSATION));
    }
    runtime.shutdown().await;
    let _ = finished_tx.send(());
    let requests = gateway.await??;
    let blocks = requests
        .iter()
        .filter_map(|request| request["messages"].as_array())
        .flat_map(|messages| messages.iter())
        .filter_map(|message| message["content"].as_str())
        .filter(|text| text.starts_with("<advisory_memory>"))
        .collect::<Vec<_>>();
    assert!(!blocks.is_empty(), "real provider must receive recall");
    assert!(blocks[0].contains(MEMORY));
    assert_eq!(blocks, vec![blocks[0]; blocks.len()]);
    if matches!(path, FailurePath::Compaction) {
        assert!(requests.iter().any(|request| request["stream"] != true
            && request["messages"].to_string().contains("advisory_memory")));
    }
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 Failure/Observability
/// Verifies: root-turn, compaction, and recovery diagnostics omit private provider text.
#[tokio::test]
async fn server_root_compaction_and_recovery_logs_omit_sensitive_errors() -> Result<()> {
    let logs = Arc::new(Mutex::new(Vec::new()));
    // One test owns this process-wide subscriber so spawned server tasks are included.
    tracing::subscriber::set_global_default(log_subscriber(
        Arc::clone(&logs),
        tracing::Level::TRACE,
    ))?;
    for path in [
        FailurePath::StreamCreation,
        FailurePath::HttpStream,
        FailurePath::StreamPayload,
        FailurePath::Compaction,
    ] {
        exercise_failure_path(path)
            .await
            .with_context(|| format!("exercise {path:?}"))?;
    }
    let logs = String::from_utf8(logs.lock().expect("logs").clone())?;
    assert!(logs.contains("turn execution failed"));
    assert!(logs.contains("LLM compaction failed"));
    assert!(logs.contains("returning protocol error"));
    let leaked_content = [MEMORY, CONVERSATION]
        .into_iter()
        .filter(|private_content| logs.contains(private_content))
        .collect::<Vec<_>>();
    assert_eq!(leaked_content, Vec::<&str>::new());
    Ok(())
}
async fn wait_notification(
    notifications: &mut tokio::sync::mpsc::Receiver<serde_json::Value>,
    method: &str,
    session: devo_protocol::SessionId,
) -> Result<serde_json::Value> {
    // Real HTTP failures exercise the existing backoff before terminal events.
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        while let Some(event) = notifications.recv().await {
            if event["method"] == method
                && (event["params"]["sessionId"] == serde_json::json!(session)
                    || event["params"]["turn"]["sessionId"] == serde_json::json!(session))
                && (method != "turn/recoveryUpdated" || event["params"]["recovery"].is_object())
            {
                return Ok(event);
            }
        }
        anyhow::bail!("notification channel closed before {method}")
    })
    .await
    .with_context(|| format!("waiting for {method}"))?
}
