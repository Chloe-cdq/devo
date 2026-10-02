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

struct HostedGate {
    entered: Arc<Notify>,
    release: Arc<Semaphore>,
}

#[async_trait::async_trait]
impl ModelProviderSDK for HostedGate {
    async fn completion(&self, _request: ModelRequest) -> Result<ModelResponse> {
        anyhow::bail!("hosted test expects a stream")
    }

    async fn completion_stream(
        &self,
        _request: ModelRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamEvent>> + Send>>> {
        use futures::StreamExt;

        let entered = Arc::clone(&self.entered);
        let release = Arc::clone(&self.release);
        let start = StreamEvent::HostedToolCallStart {
            index: 0,
            id: "hosted-web".into(),
            name: "web_search".into(),
            input: serde_json::json!({"query":"Rust"}),
        };
        let done = StreamEvent::MessageDone {
            response: ModelResponse {
                id: "hosted-response".into(),
                content: vec![ResponseContent::Text("done".into())],
                stop_reason: Some(StopReason::EndTurn),
                usage: Usage::default(),
                metadata: ResponseMetadata::default(),
            },
        };
        Ok(Box::pin(futures::stream::iter([Ok(start)]).chain(
            futures::stream::once(async move {
                entered.notify_one();
                release.acquire().await?.forget();
                Ok(done)
            }),
        )))
    }

    fn name(&self) -> &str {
        "hosted-test-provider"
    }
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7, L2-DES-SERVER-002.
/// Verifies: a live hosted-tool marker is durable while the active turn still answers read RPCs.
#[tokio::test]
async fn hosted_marker_persists_during_active_turn_without_blocking_ping() -> Result<()> {
    let root = TempDir::new()?;
    let provider = Arc::new(HostedGate {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Semaphore::new(/*permits*/ 0)),
    });
    let runtime = support::build_runtime(root.path(), provider.clone())?;
    let (connection, mut notifications) = support::initialize_connection(&runtime).await?;
    let session = support::start_session(&runtime, connection, root.path()).await?;
    let started = runtime
        .handle_incoming(
            connection,
            serde_json::json!({"id":11,"method":"turn/start","params":{
                "sessionId":session,"input":[{"type":"text","text":"Search the web"}],
                "idempotencyKey":"hosted-marker-integration"
            }}),
        )
        .await
        .context("turn/start")?;
    assert!(started.get("result").is_some());
    tokio::time::timeout(Duration::from_secs(5), provider.entered.notified()).await?;

    let ping = tokio::time::timeout(
        Duration::from_secs(1),
        runtime.handle_incoming(
            connection,
            serde_json::json!({"id":12,"method":"runtime/ping","params":{}}),
        ),
    )
    .await?
    .context("ping")?;
    assert_eq!(ping.get("error"), None);
    let db = devo_server::db::Database::open(root.path().join("goal_continuation.db"))?;
    let rollout = db
        .get_session_index(&session)?
        .context("session index")?
        .rollout_path
        .context("rollout path")?;
    let marker_count = std::fs::read_to_string(rollout)?
        .lines()
        .filter_map(|line| devo_core::parse_rollout_line(line).ok())
        .filter(|line| {
            matches!(line, devo_core::ParsedRolloutLine::V2(v2)
            if matches!(v2.as_ref(), devo_core::RolloutLineV2::Internal {
                entry: devo_core::InternalRecordV2::ExternalContextUsed, ..
            }))
        })
        .count();
    assert_eq!(marker_count, 1);

    provider.release.add_permits(1);
    tokio::time::timeout(
        Duration::from_secs(5),
        support::collect_until_turn_completed(&mut notifications),
    )
    .await??;
    Ok(())
}
