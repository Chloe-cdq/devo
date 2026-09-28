use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use devo_protocol::{
    ModelRequest, ModelResponse, ResponseContent, ResponseMetadata, StopReason, StreamEvent, Usage,
};
use devo_provider::ModelProviderSDK;
use futures::Stream;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::sync::{Notify, Semaphore};

#[path = "support/goal_continuation.rs"]
mod support;

struct BackgroundGate {
    entered: Notify,
    release: Semaphore,
}

#[async_trait::async_trait]
impl ModelProviderSDK for BackgroundGate {
    async fn completion(&self, request: ModelRequest) -> Result<ModelResponse> {
        assert!(request.tools.is_none());
        self.entered.notify_one();
        self.release.acquire().await?.forget();
        Ok(ModelResponse {
            id: "auxiliary".into(),
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
        Ok(Box::pin(futures::stream::iter([Ok(
            StreamEvent::MessageDone {
                response: ModelResponse {
                    id: "foreground".into(),
                    content: vec![ResponseContent::Text("done".into())],
                    stop_reason: Some(StopReason::EndTurn),
                    usage: Usage::default(),
                    metadata: ResponseMetadata::default(),
                },
            },
        )])))
    }
    fn name(&self) -> &str {
        "test-provider"
    }
    fn remaining_quota_percent(&self) -> Option<u8> {
        Some(100)
    }
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-6, L2-DES-SERVER-002
/// Verifies: the Native foreground turn completes and read RPCs respond while passive extraction is blocked.
#[tokio::test]
async fn foreground_turn_completes_while_extraction_is_blocked() -> Result<()> {
    let root = TempDir::new()?;
    std::fs::write(
        root.path().join("config.toml"),
        "[memory]\nenabled=true\nextract_model='test-provider/test-model'\n",
    )?;
    std::fs::write(root.path().join("providers.json"),serde_json::json!({
        "model":"test-provider/test-model",
        "provider":{"test-provider":{"name":"test-provider","wire_api":"openai_chat_completions","models":{"test-model":{"name":"test-model"}}}}
    }).to_string())?;
    let sessions = root.path().join("sessions");
    std::fs::create_dir_all(&sessions)?;
    let old = (chrono::Utc::now() - chrono::Duration::hours(7)).to_rfc3339();
    let fixture = include_str!("../../core/tests/fixtures/rollout_v1/basic_session.jsonl");
    let mut lines = fixture
        .lines()
        .take(3)
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    for line in &mut lines {
        if let Some(meta) = line.get_mut("SessionMeta") {
            meta["timestamp"] = serde_json::json!(old);
            meta["session"]["cwd"] = serde_json::json!(root.path());
            for field in ["created_at", "updated_at", "last_activity_at"] {
                meta["session"][field] = serde_json::json!(old);
            }
        }
        if let Some(turn) = line.get_mut("Turn") {
            turn["timestamp"] = serde_json::json!(old);
            for field in ["started_at", "completed_at"] {
                turn["turn"][field] = serde_json::json!(old);
            }
        }
        if let Some(item) = line.get_mut("Item") {
            item["timestamp"] = serde_json::json!(old);
            item["item"]["timestamp"] = serde_json::json!(old);
        }
    }
    std::fs::write(
        sessions.join("source-00000000-0000-0000-0000-0000000000b1.jsonl"),
        lines
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()?
            .join("\n")
            + "\n",
    )?;
    // The in-process harness does not run the executable's startup index rebuild.
    let database_path = root.path().join("goal_continuation.db");
    devo_server::db::Database::open(database_path.clone())?;
    let connection = rusqlite::Connection::open(database_path)?;
    let old_timestamp = chrono::DateTime::parse_from_rfc3339(&old)?.timestamp();
    connection.execute(
        "INSERT INTO sessions (id, cwd, created_at, updated_at, last_activity_at, rollout_path)
         VALUES (?1, ?2, ?3, ?3, ?3, ?4)",
        rusqlite::params![
            "00000000-0000-0000-0000-0000000000b1",
            root.path().to_string_lossy(),
            old_timestamp,
            sessions
                .join("source-00000000-0000-0000-0000-0000000000b1.jsonl")
                .to_string_lossy()
        ],
    )?;
    drop(connection);
    let provider = Arc::new(BackgroundGate {
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let runtime = support::build_runtime(root.path(), provider.clone())?;
    let (connection, mut notifications) = support::initialize_connection(&runtime).await?;
    let session = tokio::time::timeout(
        Duration::from_secs(5),
        support::start_session(&runtime, connection, root.path()),
    )
    .await??;
    tokio::time::timeout(Duration::from_secs(5), provider.entered.notified()).await?;
    let started=runtime.handle_incoming(connection,serde_json::json!({"id":3,"method":"turn/start","params":{
        "sessionId":session,"input":[{"type":"text","text":"hello"}],"idempotencyKey":"foreground-during-memory"
    }})).await.context("turn/start")?;
    assert!(started.get("result").is_some());
    tokio::time::timeout(
        Duration::from_secs(5),
        support::collect_until_turn_completed(&mut notifications),
    )
    .await??;
    let ping = tokio::time::timeout(
        Duration::from_secs(1),
        runtime.handle_incoming(
            connection,
            serde_json::json!({"id":4,"method":"runtime/ping","params":{}}),
        ),
    )
    .await?
    .context("ping")?;
    assert_eq!(ping.get("error"), None);
    provider.release.add_permits(1);
    Ok(())
}
