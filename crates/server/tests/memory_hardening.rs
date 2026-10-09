use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use devo_core::MemoryConfig;
use devo_server::memory::MemoryRuntime;
use pretty_assertions::assert_eq;
use serde_json::json;

#[path = "../../core/tests/support/memory_log_privacy.rs"]
#[allow(dead_code)]
mod log_support;
#[path = "support/memory_forget_runtime.rs"]
#[allow(dead_code)]
mod memory_support;
#[path = "support/subagent_lifecycle.rs"]
#[allow(dead_code)]
mod support;

/// Trace: L2-DES-MEM-001 Rev 4 Privacy and Authority / Failure and Observability.
/// Verifies: private storage diagnostics never enter logs; failed memory leaves root and automation turns available.
#[tokio::test]
async fn storage_failures_are_content_free_and_isolated_from_foreground() -> Result<()> {
    const PRIVATE: &str = "private-memory-and-transcript-marker";
    let logs = Arc::new(Mutex::new(Vec::new()));
    // Include startup reconciliation and spawned turn tasks in this test process.
    tracing::subscriber::set_global_default(log_support::log_subscriber(
        Arc::clone(&logs),
        tracing::Level::WARN,
    ))?;

    for initialization_failure in [false, true] {
        let data = memory_support::configured_data_root()?;
        let memory_root = data.path().join("memory");
        drop(MemoryRuntime::open(
            memory_root.clone(),
            MemoryConfig {
                enabled: true,
                ..Default::default()
            },
        )?);
        let db = rusqlite::Connection::open(memory_root.join("memory.sqlite3"))?;
        if initialization_failure {
            db.execute_batch(&format!(
                "CREATE TRIGGER reject_maintenance BEFORE DELETE ON memory_candidates
                 BEGIN SELECT RAISE(ABORT, '{PRIVATE}'); END;
                 INSERT INTO memory_candidates(candidate_id,scope_type,scope_id,kind,normalized_key,
                    body,origin,source_session_id,retention_until,created_at)
                 VALUES ('expired','user','user','fact','private','private','inferred_session',
                    'source','2000-01-01T00:00:00Z','2000-01-01T00:00:00Z');"
            ))?;
        } else {
            db.execute(
                "INSERT INTO memory_job_receipts(source_session_id,source_watermark,completed_at,job_kind)
                 VALUES ('source','finished',?1,'source_scan')",
                [PRIVATE],
            )?;
        }
        let provider = Arc::new(support::ScriptedProvider::new([
            support::ScriptedProvider::completed("root survived"),
            support::ScriptedProvider::completed("automation survived"),
        ]));
        let runtime = support::build_runtime_with_workspace_config(data.path(), provider.clone())?;
        let (connection, mut notifications) = support::initialize_connection(&runtime).await?;
        let status = runtime
            .handle_incoming(
                connection,
                json!({
                    "id": 10, "method": "memory/status", "params": {}
                }),
            )
            .await
            .context("memory status")?;
        assert_eq!(
            status["result"],
            json!({
                "enabled": true, "storageHealth": "unavailable", "entryCount": 0,
                "candidateCount": 0, "pendingJobCount": 0, "retryingJobCount": 0,
                "errorJobCount": 0, "lastSuccessfulScanAt": null,
                "errorClasses": [], "sourceExclusionReasons": []
            })
        );
        assert!(!status.to_string().contains(PRIVATE));

        if !initialization_failure {
            db.execute_batch(&format!(
                "CREATE TRIGGER reject_scan_maintenance BEFORE DELETE ON memory_candidates
                 BEGIN SELECT RAISE(ABORT, '{PRIVATE}'); END;
                 INSERT INTO memory_candidates(candidate_id,scope_type,scope_id,kind,normalized_key,
                    body,origin,source_session_id,retention_until,created_at)
                 VALUES ('expired','user','user','fact','private','private','inferred_session',
                    'source','2000-01-01T00:00:00Z','2000-01-01T00:00:00Z');"
            ))?;
        }
        for source in ["interactive", "automation"] {
            let response = runtime
                .handle_incoming(
                    connection,
                    json!({
                        "id": 11, "method": "session/new", "params": {
                            "cwd": data.path(), "source": source, "idempotencyKey": source
                        }
                    }),
                )
                .await
                .context("new session")?;
            let session = serde_json::from_value(response["result"]["session"]["id"].clone())?;
            let response = runtime.handle_incoming(connection, json!({
                "id": 12, "method": "subscription/create", "params": {
                    "selectors": [{"kind": "session", "sessionId": session}], "includeSnapshot": false
                }
            })).await.context("subscribe")?;
            anyhow::ensure!(
                response.get("result").is_some(),
                "subscription failed: {response}"
            );
            support::start_turn_with_approval_policy(
                &runtime,
                connection,
                session,
                "Continue the task",
                Some("never"),
            )
            .await?;
            let completed = support::wait_for_session_notification(
                &mut notifications,
                "turn/completed",
                session,
            )
            .await?;
            assert_eq!(completed["params"]["turn"]["status"], json!("completed"));
        }
        assert_eq!(provider.requests().len(), 2);
        if !initialization_failure {
            tokio::time::timeout(std::time::Duration::from_secs(/*secs*/ 5), async {
                loop {
                    let captured = logs.lock().expect("logs").clone();
                    if String::from_utf8_lossy(&captured).contains("background memory scan failed")
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .context("background failure diagnostic")?;
        }
        runtime.shutdown().await;
    }
    let logs = String::from_utf8(logs.lock().expect("logs").clone())?;
    assert!(logs.contains("memory status unavailable"));
    assert!(logs.contains("failed to initialize persistent memory runtime"));
    assert!(
        !logs.contains(PRIVATE),
        "storage errors leaked private content"
    );
    Ok(())
}
