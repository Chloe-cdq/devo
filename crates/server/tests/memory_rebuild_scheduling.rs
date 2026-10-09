use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use anyhow::{Context, Result};
use devo_protocol::{
    ModelRequest, ModelResponse, ResponseContent, ResponseMetadata, StopReason, StreamEvent, Usage,
};
use devo_provider::ModelProviderSDK;
use futures::Stream;
use pretty_assertions::assert_eq;
use serde_json::{Value, json};
use tokio::sync::Notify;

#[path = "support/goal_continuation.rs"]
mod support;

struct Extractor {
    calls: AtomicUsize,
    entered: Notify,
}

#[async_trait::async_trait]
impl ModelProviderSDK for Extractor {
    async fn completion(&self, request: ModelRequest) -> Result<ModelResponse> {
        anyhow::ensure!(
            serde_json::to_string(&request)?.contains("eligible-project-b-history"),
            "only Project B's eligible history should be extracted"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        Ok(ModelResponse {
            id: "extraction".into(),
            content: vec![ResponseContent::Text("{\"candidates\":[]}".into())],
            stop_reason: Some(StopReason::EndTurn),
            usage: Usage::default(),
            metadata: ResponseMetadata::default(),
        })
    }

    async fn completion_stream(
        &self,
        _request: ModelRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamEvent>> + Send>>> {
        anyhow::bail!("this test only expects background extraction")
    }

    fn name(&self) -> &str {
        "test-provider"
    }
    fn remaining_quota_percent(&self) -> Option<u8> {
        Some(100)
    }
}

/// Trace: L2-DES-MEM-001 DD-6, DD-9, L2-DES-SERVER-002.
/// Verifies: a normal Native session extracts another Project's eligible history while a rebuild remains deferred.
#[tokio::test]
async fn normal_session_scans_other_project_while_rebuild_is_deferred() -> Result<()> {
    let root = tempfile::tempdir()?;
    let project_a = root.path().join("project-a");
    let project_b = root.path().join("project-b");
    std::fs::create_dir_all(&project_a)?;
    std::fs::create_dir_all(&project_b)?;
    std::fs::write(
        root.path().join("config.toml"),
        "[memory]\nenabled=true\nextract_model='test-provider/test-model'\n",
    )?;
    std::fs::write(
        root.path().join("providers.json"),
        json!({
            "model":"test-provider/test-model", "provider":{"test-provider":{
                "name":"test-provider", "wire_api":"openai_chat_completions",
                "models":{"test-model":{"name":"test-model"}}
            }}
        })
        .to_string(),
    )?;
    let sessions = root.path().join("sessions");
    std::fs::create_dir_all(&sessions)?;
    let database_path = root.path().join("goal_continuation.db");
    devo_server::db::Database::open(database_path.clone())?;
    let db = rusqlite::Connection::open(database_path)?;
    let source_a = "00000000-0000-0000-0000-0000000000b1";
    for (id, workspace, age, message) in [
        (source_a, &project_a, 3, "recent-project-a-history"),
        (
            "00000000-0000-0000-0000-0000000000c1",
            &project_b,
            7,
            "eligible-project-b-history",
        ),
    ] {
        let observed = chrono::Utc::now() - chrono::Duration::hours(age);
        let fixture = include_str!("../../core/tests/fixtures/rollout_v1/basic_session.jsonl");
        let mut lines = fixture
            .lines()
            .take(3)
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        for line in &mut lines {
            if let Some(meta) = line.get_mut("SessionMeta") {
                meta["timestamp"] = json!(observed);
                meta["session"]["id"] = json!(id);
                meta["session"]["cwd"] = json!(workspace);
                for field in ["created_at", "updated_at", "last_activity_at"] {
                    meta["session"][field] = json!(observed);
                }
            }
            if let Some(turn) = line.get_mut("Turn") {
                turn["timestamp"] = json!(observed);
                turn["turn"]["session_id"] = json!(id);
                for field in ["started_at", "completed_at"] {
                    turn["turn"][field] = json!(observed);
                }
            }
            if let Some(item) = line.get_mut("Item") {
                item["timestamp"] = json!(observed);
                item["item"]["timestamp"] = json!(observed);
                item["item"]["session_id"] = json!(id);
                item["item"]["input_items"][0]["UserMessage"]["text"] = json!(message);
            }
        }
        let path = sessions.join(format!("source-{id}.jsonl"));
        std::fs::write(
            &path,
            lines
                .iter()
                .map(serde_json::to_string)
                .collect::<Result<Vec<_>, _>>()?
                .join("\n")
                + "\n",
        )?;
        db.execute(
            "INSERT INTO sessions (id, cwd, created_at, updated_at, last_activity_at, rollout_path)
            VALUES (?1, ?2, ?3, ?3, ?3, ?4)",
            rusqlite::params![
                id,
                workspace.to_string_lossy(),
                observed.timestamp(),
                path.to_string_lossy()
            ],
        )?;
    }
    drop(db);
    let provider = Arc::new(Extractor {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
    });
    let runtime = support::build_runtime(root.path(), provider.clone())?;
    let (connection, _notifications) = support::initialize_connection(&runtime).await?;
    let subscribed = runtime
        .handle_incoming(
            connection,
            json!({
                "id":1,"method":"subscription/create","params":{
                    "selectors":[{"kind":"session","sessionId":source_a}],"includeSnapshot":false
                }
            }),
        )
        .await
        .context("bind Project A")?;
    anyhow::ensure!(
        subscribed.get("result").is_some(),
        "subscription: {subscribed}"
    );
    let accepted = runtime
        .handle_incoming(
            connection,
            json!({
                "id":2,"method":"memory/rebuild","params":{"scope":"project"}
            }),
        )
        .await
        .context("rebuild Project A")?;
    anyhow::ensure!(accepted.get("result").is_some(), "rebuild: {accepted}");
    support::start_session(&runtime, connection, &project_b).await?;
    tokio::time::timeout(Duration::from_secs(5), provider.entered.notified())
        .await
        .context("ordinary Project B extraction was blocked by the deferred Project A rebuild")?;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let status = runtime
        .handle_incoming(
            connection,
            json!({
                "id":3,"method":"memory/status","params":{}
            }),
        )
        .await
        .context("status")?;
    assert_eq!(
        status["result"]["rebuild"],
        json!({
            "pendingRequestCount":1,"pendingJobCount":0,"runningJobCount":0,
            "retryingJobCount":0,"completedJobCount":0,"errorJobCount":0
        })
    );
    runtime.shutdown().await;
    Ok(())
}
