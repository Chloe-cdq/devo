use super::*;

#[path = "memory_scan_latency_tests.rs"]
mod latency_tests;
use anyhow::{Context, Result};
use chrono::Utc;
use devo_core::tools::ToolRegistry;
use devo_core::{
    AgentsMdConfig, AppConfigStore, BundledSkillsConfig, FileSystemSkillCatalog,
    PresetModelCatalog, SkillsConfig,
};
use devo_protocol::{
    Model, ModelRequest, ModelResponse, ResponseContent, ResponseMetadata, StopReason, StreamEvent,
    Usage,
};
use devo_provider::{ModelProviderSDK, SingleProviderRouter};
use futures::Stream;
use pretty_assertions::assert_eq;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::{Notify, Semaphore};

struct BlockingExtractor {
    entered: Notify,
    release: Semaphore,
    calls: AtomicUsize,
    requests: std::sync::Mutex<Vec<ModelRequest>>,
    quota: std::sync::Mutex<Option<u8>>,
    failure: std::sync::Mutex<Option<devo_provider::error::ProviderError>>,
}
#[async_trait::async_trait]
impl ModelProviderSDK for BlockingExtractor {
    async fn completion(&self, request: ModelRequest) -> Result<ModelResponse> {
        assert_eq!(request.model, "test-fast");
        assert!(request.tools.is_none());
        self.requests.lock().unwrap().push(request);
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
        if let Some(error) = self.failure.lock().unwrap().clone() {
            return Err(error.into());
        }
        Ok(ModelResponse {
            id: "extraction-response".into(),
            content: vec![ResponseContent::Text(serde_json::json!({
                "candidates": [{"scope":"user","kind":"preference","key":"indentation","body":"I prefer tabs","evidence":["00000000-0000-0000-0000-0000000000b2"]}]
            }).to_string())],
            stop_reason: Some(StopReason::EndTurn),
            usage: Usage::default(), metadata: ResponseMetadata::default(),
        })
    }
    async fn completion_stream(
        &self,
        _request: ModelRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamEvent>> + Send>>> {
        Ok(Box::pin(futures::stream::empty()))
    }
    fn name(&self) -> &str {
        "test-provider"
    }
    fn remaining_quota_percent(&self) -> Option<u8> {
        *self.quota.lock().unwrap()
    }
}

fn setup(
    sources: usize,
    permits: usize,
) -> Result<(TempDir, Arc<ServerRuntime>, Arc<BlockingExtractor>)> {
    let root = TempDir::new()?;
    std::fs::write(
        root.path().join("config.toml"),
        "[memory]\nenabled = true\nextract_model = 'test-provider/test-fast'\n",
    )?;
    std::fs::write(root.path().join("providers.json"), serde_json::json!({
        "model":"test-provider/test-fast",
        "provider":{"test-provider":{"name":"test-provider","wire_api":"openai_chat_completions",
            "request":{"provider_field":"required","nested":{"provider":true}},
            "options":{"option_field":"required"},
            "models":{"test-fast":{
                "name":"test-fast",
                "headers":{"X-Model":"required","X-Mode":"model"},
                "request":{"nested":{"model":true}},
                "default_variant":"fast",
                "variants":{"fast":{
                    "headers":{"X-Mode":"variant"},
                    "options":{"nested":{"variant":true}},
                    "request":{"tools":[{"type":"web_search"}],"tool_choice":"required",
                        "thinking":{"type":"enabled"},"__devo_background_request":false,
                        "previous_response_id":"resp-other-session",
                        "conversation":"conv-other-session",
                        "prompt":{"id":"pmpt-other-session"},
                        "messages":[{"role":"system","content":"override"}],
                        "input":"override","instructions":"override","max_output_tokens":1}
                }}
            }}}}
    }).to_string())?;
    let sessions = root.path().join("sessions");
    std::fs::create_dir_all(&sessions)?;
    let old = (Utc::now() - chrono::Duration::hours(7)).to_rfc3339();
    for _ in 0..sources {
        let id = SessionId::new();
        let fixture = include_str!("../../../core/tests/fixtures/rollout_v1/basic_session.jsonl");
        let mut lines = fixture
            .lines()
            .take(3)
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<Result<Vec<_>, _>>()?;
        for line in &mut lines {
            if let Some(meta) = line.get_mut("SessionMeta") {
                meta["timestamp"] = serde_json::json!(old);
                meta["session"]["id"] = serde_json::json!(id);
                meta["session"]["cwd"] = serde_json::json!(root.path());
                meta["session"]["created_at"] = serde_json::json!(old);
                meta["session"]["updated_at"] = serde_json::json!(old);
                meta["session"]["last_activity_at"] = serde_json::json!(old);
            }
            if let Some(turn) = line.get_mut("Turn") {
                turn["timestamp"] = serde_json::json!(old);
                turn["turn"]["session_id"] = serde_json::json!(id);
                turn["turn"]["started_at"] = serde_json::json!(old);
                turn["turn"]["completed_at"] = serde_json::json!(old);
            }
            if let Some(item) = line.get_mut("Item") {
                item["timestamp"] = serde_json::json!(old);
                item["item"]["session_id"] = serde_json::json!(id);
                item["item"]["timestamp"] = serde_json::json!(old);
                item["item"]["input_items"][0]["UserMessage"]["text"] =
                    serde_json::json!("I prefer tabs");
            }
        }
        let journal = lines
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()?
            .join("\n")
            + "\n";
        std::fs::write(sessions.join(format!("source-{id}.jsonl")), journal)?;
    }
    let provider = Arc::new(BlockingExtractor {
        entered: Notify::new(),
        release: Semaphore::new(permits),
        calls: AtomicUsize::new(0),
        requests: std::sync::Mutex::new(Vec::new()),
        quota: std::sync::Mutex::new(Some(100)),
        failure: std::sync::Mutex::new(None),
    });
    let sdk: Arc<dyn ModelProviderSDK> = provider.clone();
    let runtime = ServerRuntime::new(
        root.path().into(),
        ServerRuntimeDependencies::new(
            Arc::clone(&sdk),
            Arc::new(SingleProviderRouter::new(sdk)),
            Arc::new(ToolRegistry::new()),
            crate::empty_mcp_manager(),
            "test-fast".into(),
            Arc::new(PresetModelCatalog::new(vec![Model {
                slug: "test-fast".into(),
                display_name: "test-fast".into(),
                ..Model::default()
            }])),
            Box::new(FileSystemSkillCatalog::new(SkillsConfig {
                bundled: Some(BundledSkillsConfig { enabled: false }),
                ..SkillsConfig::default()
            })),
            AgentsMdConfig::default(),
            Arc::new(crate::db::Database::open(root.path().join("devo.db"))?),
            Arc::new(std::sync::Mutex::new(AppConfigStore::load(
                root.path().into(),
                /*workspace_root*/ None,
            )?)),
        ),
    );
    runtime
        .rollout_store
        .index_rollout_metadata(&runtime.deps.db)?;
    assert_eq!(runtime.deps.db.list_root_sessions()?.len(), sources);
    Ok((root, runtime, provider))
}

async fn connect(runtime: &Arc<ServerRuntime>) -> Result<u64> {
    let (tx, _rx) = crate::test_outbound_channel(64);
    let id = runtime
        .register_connection(ClientTransportKind::Stdio, tx)
        .await;
    let response = runtime.handle_incoming(id, serde_json::json!({
        "id":0,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{},
        "_meta":{"devo":{"protocol":"native"}},"clientInfo":{"name":"test","title":"test","version":"1"}}
    })).await.context("initialize")?;
    anyhow::ensure!(response.get("result").is_some(), "initialize rejected");
    Ok(id)
}

async fn start(
    runtime: &Arc<ServerRuntime>,
    connection: u64,
    root: &std::path::Path,
) -> Result<serde_json::Value> {
    runtime.handle_incoming(connection, serde_json::json!({
        "id":1,"method":"session/new","params":{"cwd":root,"idempotencyKey":uuid::Uuid::new_v4().to_string()}
    })).await.context("session new")
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-6, Operational Scheduling
/// Verifies: session creation and runtime reads remain responsive while the auxiliary extractor is blocked.
#[tokio::test]
async fn root_start_and_ping_do_not_wait_for_background_extraction() -> Result<()> {
    let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 0)?;
    let connection = connect(&runtime).await?;
    let started = tokio::time::timeout(
        Duration::from_secs(5),
        start(&runtime, connection, root.path()),
    )
    .await??;
    assert!(started.get("result").is_some());
    tokio::time::timeout(Duration::from_secs(5), provider.entered.notified()).await?;
    let ping = tokio::time::timeout(
        Duration::from_secs(1),
        runtime.handle_incoming(
            connection,
            serde_json::json!({
                "id":2,"method":"runtime/ping","params":{}
            }),
        ),
    )
    .await?
    .context("ping")?;
    assert!(ping.get("result").is_some());
    let turn = tokio::time::timeout(Duration::from_secs(2), runtime.handle_incoming(connection, serde_json::json!({
        "id":3,"method":"turn/start","params":{"sessionId":started["result"]["session"]["id"],
            "input":[{"type":"text","text":"hello"}],"idempotencyKey":"foreground-during-extraction"}
    }))).await?.context("turn/start")?;
    assert!(
        turn.get("result").is_some(),
        "foreground turn rejected: {turn}"
    );
    provider.release.add_permits(1);
    Ok(())
}

/// Trace: L2-DES-MEM-001 Operational Scheduling
/// Verifies: the default per-start source cap admits two of three eligible journals.
#[tokio::test]
async fn root_start_caps_background_sources_at_two() -> Result<()> {
    let (root, runtime, provider) = setup(/*sources*/ 3, /*permits*/ 3)?;
    let connection = connect(&runtime).await?;
    assert!(
        start(&runtime, connection, root.path())
            .await?
            .get("result")
            .is_some()
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let result = runtime
                .memory
                .as_ref()
                .unwrap()
                .execute_command(crate::memory::MemoryCommand::Status)
                .await
                .unwrap();
            if let crate::memory::MemoryCommandResult::Status(status) = result
                && status.last_successful_scan_at.is_some()
                && provider.calls.load(Ordering::SeqCst) == 2
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    Ok(())
}

struct IdleSources;
#[async_trait::async_trait]
impl crate::memory::scan::SourceActivity for IdleSources {
    async fn is_active(&self, _session_id: SessionId) -> bool {
        false
    }
}

async fn scan(runtime: &Arc<ServerRuntime>, root: &std::path::Path) -> Result<()> {
    let context = crate::memory::scan::ScanContext {
        db: Arc::clone(&runtime.deps.db),
        model_context: runtime.deps.context_for_workspace(root).await?,
        usage_ledger: runtime.usage_ledger.clone(),
        triggering_session: SessionId::new(),
        activity: Arc::new(IdleSources),
    };
    Arc::clone(runtime.memory.as_ref().unwrap())
        .run_background_scan(context)
        .await
}

/// Trace: L2-DES-MEM-001 Entry Lifecycle and Retention
/// Verifies: projection repair cannot clear a durable deletion fence while source metadata still exists.
#[tokio::test]
async fn reconciliation_keeps_deletion_intent_until_session_is_gone() -> Result<()> {
    let (root, runtime, _) = setup(/*sources*/ 1, /*permits*/ 1)?;
    let source_id = runtime.deps.db.list_root_sessions()?[0].session_id;
    scan(&runtime, root.path()).await?;
    runtime
        .deps
        .db
        .record_memory_source_deletions(&[source_id])?;

    runtime.memory.as_ref().unwrap().reconcile_source_intents();

    assert!(runtime.deps.db.get_session(&source_id)?.is_some());
    assert_eq!(
        runtime.deps.db.pending_memory_source_deletions()?,
        vec![source_id]
    );
    Ok(())
}

/// Trace: L2-DES-MEM-001 Entry Lifecycle and Retention
/// Verifies: deleting a processed source retires its inferred entry and removes source detail.
#[tokio::test]
async fn deleting_processed_source_removes_its_memory() -> Result<()> {
    let (root, runtime, _) = setup(/*sources*/ 1, /*permits*/ 1)?;
    let source_id = runtime.deps.db.list_root_sessions()?[0].session_id;
    scan(&runtime, root.path()).await?;
    let memory = runtime.memory.as_ref().unwrap();
    let before = memory
        .execute_command(crate::memory::MemoryCommand::List(
            crate::memory::ListMemoryRequest::default(),
        ))
        .await?;
    let crate::memory::MemoryCommandResult::List(before) = before else {
        panic!("list result")
    };
    assert_eq!(before.data.len(), 1);

    assert_eq!(
        runtime
            .delete_session_tree(source_id)
            .await
            .map_err(anyhow::Error::msg)?,
        vec![source_id]
    );
    memory.reconcile_source_intents();
    let after = memory
        .execute_command(crate::memory::MemoryCommand::List(
            crate::memory::ListMemoryRequest::default(),
        ))
        .await?;
    let crate::memory::MemoryCommandResult::List(after) = after else {
        panic!("list result")
    };
    let entries = after.data;
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].state,
        devo_protocol::native::rpc_memory::MemoryState::Retired
    );
    let connection = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
    let counts: (i64, i64, i64) = connection.query_row(
        "SELECT (SELECT COUNT(*) FROM memory_candidates),
                (SELECT COUNT(*) FROM memory_evidence),
                (SELECT COUNT(*) FROM memory_jobs)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(counts, (0, 0, 0));
    Ok(())
}

/// Trace: L2-DES-MEM-001 Entry Lifecycle and Retention
/// Verifies: deletion wins over an extractor response already in flight.
#[tokio::test]
async fn deleting_source_during_extraction_prevents_late_commit() -> Result<()> {
    let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 0)?;
    let source_id = runtime.deps.db.list_root_sessions()?[0].session_id;
    let path = root.path().to_path_buf();
    let scanning = Arc::clone(&runtime);
    let task = tokio::spawn(async move { scan(&scanning, &path).await });
    tokio::time::timeout(Duration::from_secs(10), provider.entered.notified()).await?;

    assert_eq!(
        runtime
            .delete_session_tree(source_id)
            .await
            .map_err(anyhow::Error::msg)?,
        vec![source_id]
    );
    provider.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(10), task).await???;

    let memory = runtime.memory.as_ref().unwrap();
    let result = memory
        .execute_command(crate::memory::MemoryCommand::List(
            crate::memory::ListMemoryRequest::default(),
        ))
        .await?;
    let crate::memory::MemoryCommandResult::List(entries) = result else {
        panic!("list result")
    };
    assert_eq!(entries.data, vec![]);
    memory.reconcile_source_intents();
    let connection = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
    let job_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM memory_jobs WHERE source_session_id = ?1",
        [source_id.to_string()],
        |row| row.get(0),
    )?;
    assert_eq!(job_count, 0);
    Ok(())
}

/// Trace: L2-DES-MEM-001 Entry Lifecycle and Retention
/// Verifies: a projection failure cannot block session deletion; projection repair is retryable.
#[tokio::test]
async fn source_delete_retries_after_projection_write_failure() -> Result<()> {
    let (root, runtime, _) = setup(/*sources*/ 1, /*permits*/ 1)?;
    let source_id = runtime.deps.db.list_root_sessions()?[0].session_id;
    scan(&runtime, root.path()).await?;
    let projection_dir = root.path().join("memory/user");
    std::fs::remove_file(projection_dir.join("MEMORY.md"))?;
    std::fs::remove_dir(&projection_dir)?;
    std::fs::write(&projection_dir, "blocked")?;

    assert_eq!(
        runtime
            .delete_session_tree(source_id)
            .await
            .map_err(anyhow::Error::msg)?,
        vec![source_id]
    );
    assert!(runtime.deps.db.get_session(&source_id)?.is_none());
    assert_eq!(
        runtime.deps.db.pending_memory_source_deletions()?,
        vec![source_id]
    );
    std::fs::remove_file(&projection_dir)?;
    runtime.memory.as_ref().unwrap().reconcile_source_intents();
    assert_eq!(runtime.deps.db.pending_memory_source_deletions()?, vec![]);
    let projection = std::fs::read_to_string(projection_dir.join("MEMORY.md"))?;
    assert!(projection.contains("state: retired"));
    Ok(())
}

/// Trace: L1-REQ-MEM-001 Session Deletion; L2-DES-MEM-001 Entry Lifecycle and Retention.
/// Verifies: memory outage cannot block session deletion and the intent survives until repair.
#[tokio::test]
async fn source_delete_survives_memory_storage_error() -> Result<()> {
    let (root, runtime, _) = setup(/*sources*/ 1, /*permits*/ 1)?;
    let source_id = runtime.deps.db.list_root_sessions()?[0].session_id;
    scan(&runtime, root.path()).await?;
    let connection = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
    connection.execute("DROP TABLE memory_deleted_sources", [])?;

    assert_eq!(
        runtime
            .delete_session_tree(source_id)
            .await
            .map_err(anyhow::Error::msg)?,
        vec![source_id]
    );
    assert!(runtime.deps.db.get_session(&source_id)?.is_none());
    assert_eq!(
        runtime.deps.db.pending_memory_source_deletions()?,
        vec![source_id]
    );

    connection.execute(
        "CREATE TABLE memory_deleted_sources (
            source_session_id TEXT PRIMARY KEY NOT NULL,
            deleted_at TEXT NOT NULL)",
        [],
    )?;
    let memory = runtime.memory.as_ref().unwrap();
    memory.reconcile_source_intents();
    assert_eq!(runtime.deps.db.pending_memory_source_deletions()?, vec![]);
    let result = memory
        .execute_command(crate::memory::MemoryCommand::List(
            crate::memory::ListMemoryRequest::default(),
        ))
        .await?;
    let crate::memory::MemoryCommandResult::List(entries) = result else {
        panic!("list result")
    };
    assert_eq!(
        entries.data[0].state,
        devo_protocol::native::rpc_memory::MemoryState::Retired
    );
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: the durable external-context ledger excludes a source even without a rollout marker.
#[tokio::test]
async fn external_context_ledger_blocks_scan_without_rollout_marker() -> Result<()> {
    let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 1)?;
    let source_id = runtime.deps.db.list_root_sessions()?[0].session_id;
    runtime
        .deps
        .db
        .record_external_context_sources(&[source_id])?;

    scan(&runtime, root.path()).await?;

    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    runtime.memory.as_ref().unwrap().reconcile_source_intents();
    let connection = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
    let excluded: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM memory_excluded_sources WHERE source_session_id = ?1)",
        [source_id.to_string()],
        |row| row.get(0),
    )?;
    assert!(excluded);
    Ok(())
}

/// Trace: L2-DES-MEM-001 Operational Scheduling
/// Verifies: unknown and below-threshold quotas never claim or call, while exactly 25 percent admits a source.
#[tokio::test]
async fn scan_requires_known_quota_at_threshold() -> Result<()> {
    let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 1)?;
    for quota in [None, Some(24)] {
        *provider.quota.lock().unwrap() = quota;
        scan(&runtime, root.path()).await?;
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    }
    *provider.quota.lock().unwrap() = Some(25);
    scan(&runtime, root.path()).await?;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-6/DD-9
/// Verifies: a source invalidated during extraction contributes nothing and its old watermark is completed once.
#[tokio::test]
async fn changing_source_during_extraction_discards_candidates() -> Result<()> {
    let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 0)?;
    let worker_runtime = Arc::clone(&runtime);
    let worker_root = root.path().to_path_buf();
    let worker = tokio::spawn(async move { scan(&worker_runtime, &worker_root).await });
    tokio::time::timeout(Duration::from_secs(5), provider.entered.notified()).await?;
    let source = std::fs::read_dir(root.path().join("sessions"))?
        .next()
        .unwrap()?
        .path();
    std::fs::write(source, "damaged journal\n")?;
    provider.release.add_permits(1);
    worker.await??;
    let status = runtime
        .memory
        .as_ref()
        .unwrap()
        .execute_command(crate::memory::MemoryCommand::Status)
        .await?;
    let crate::memory::MemoryCommandResult::Status(status) = status else {
        panic!("status result")
    };
    assert_eq!(
        (
            status.entry_count,
            status.pending_job_count,
            status.error_job_count
        ),
        (0, 0, 0)
    );
    assert!(status.last_successful_scan_at.is_some());
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-6, Operational Scheduling
/// Verifies: a permanent provider error is terminal and safely visible while the foreground runtime remains usable.
#[tokio::test]
async fn permanent_failure_is_safe_and_does_not_break_foreground() -> Result<()> {
    let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 1)?;
    *provider.failure.lock().unwrap() =
        Some(devo_provider::error::ProviderError::AuthenticationError {
            message: "raw provider body with credential sk-secret-test-value".into(),
            provider_name: None,
            status_code: Some(401),
        });
    scan(&runtime, root.path()).await?;
    scan(&runtime, root.path()).await?;
    let status = runtime
        .memory
        .as_ref()
        .unwrap()
        .execute_command(crate::memory::MemoryCommand::Status)
        .await?;
    let crate::memory::MemoryCommandResult::Status(status) = status else {
        panic!("status result")
    };
    assert_eq!(
        (
            provider.calls.load(Ordering::SeqCst),
            status.error_job_count,
            status.error_classes
        ),
        (1, 1, vec!["credentials_unavailable".to_string()])
    );
    let connection = connect(&runtime).await?;
    let ping = runtime
        .handle_incoming(
            connection,
            serde_json::json!({"id":2,"method":"runtime/ping","params":{}}),
        )
        .await
        .context("ping")?;
    assert!(ping.get("result").is_some());
    Ok(())
}
struct InvalidatingSource {
    checks: AtomicUsize,
    path: std::path::PathBuf,
}
#[async_trait::async_trait]
impl crate::memory::scan::SourceActivity for InvalidatingSource {
    async fn is_active(&self, _session_id: SessionId) -> bool {
        if self.checks.fetch_add(1, Ordering::SeqCst) == 1 {
            std::fs::write(&self.path, "damaged journal\n").unwrap();
        }
        false
    }
}

/// Trace: L2-DES-MEM-001 DD-6/DD-9
/// Verifies: persisted eligibility is rechecked after claiming and before sending the transcript.
#[tokio::test]
async fn invalidated_claim_never_sends_transcript() -> Result<()> {
    let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 1)?;
    let path = std::fs::read_dir(root.path().join("sessions"))?
        .next()
        .unwrap()?
        .path();
    let context = crate::memory::scan::ScanContext {
        db: Arc::clone(&runtime.deps.db),
        model_context: runtime.deps.context_for_workspace(root.path()).await?,
        usage_ledger: runtime.usage_ledger.clone(),
        triggering_session: SessionId::new(),
        activity: Arc::new(InvalidatingSource {
            checks: AtomicUsize::new(0),
            path,
        }),
    };
    Arc::clone(runtime.memory.as_ref().unwrap())
        .run_background_scan(context)
        .await?;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    Ok(())
}
/// Trace: L2-DES-MEM-001 DD-6
/// Verifies: a missing indexed source cannot prevent a later eligible source from being processed.
#[tokio::test]
async fn missing_source_does_not_abort_other_sources() -> Result<()> {
    let (root, runtime, provider) = setup(/*sources*/ 2, /*permits*/ 2)?;
    let first = runtime.deps.db.list_root_sessions()?[0].session_id;
    let path = runtime
        .deps
        .db
        .get_session_index(&first)?
        .unwrap()
        .rollout_path
        .unwrap();
    std::fs::remove_file(path)?;
    scan(&runtime, root.path()).await?;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-6
/// Verifies: extraction inherits the resolved request overlays and headers while remaining tool-free.
#[tokio::test]
async fn scan_preserves_model_request_configuration() -> Result<()> {
    let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 1)?;
    scan(&runtime, root.path()).await?;
    let requests = provider.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(
        (
            request.extra_body.clone(),
            serde_json::to_value(&request.tools)?,
            request.hosted_tools.clone(),
            request.request_thinking.clone(),
        ),
        (
            Some(serde_json::json!({
                "provider_field":"required",
                "option_field":"required",
                "nested":{"provider":true,"model":true,"variant":true},
                "__devo_request_headers":{"X-Model":"required","X-Mode":"variant"},
                "__devo_background_request":true,
            })),
            serde_json::Value::Null,
            Vec::new(),
            Some("disabled".to_string()),
        )
    );
    Ok(())
}
/// Trace: L2-DES-MEM-001 Operational Scheduling
/// Verifies: known missing credentials are terminal and redacted even without quota telemetry.
#[tokio::test]
async fn scan_reports_missing_credentials_without_quota() -> Result<()> {
    for extraction_provider in [
        serde_json::json!({
            "credential":"missing-private-credential",
            "wire_api":"openai_chat_completions",
            "models":{"aux":{"name":"aux"}}
        }),
        serde_json::json!({
            "wire_api":"anthropic_messages",
            "models":{"aux":{"name":"aux"}}
        }),
    ] {
        let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 0)?;
        let providers_path = root.path().join("providers.json");
        let mut config: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&providers_path)?)?;
        config["provider"]["unavailable"] = extraction_provider;
        std::fs::write(providers_path, config.to_string())?;
        let memory = Arc::new(crate::memory::MemoryRuntime::open(
            root.path().join("memory"),
            devo_core::MemoryConfig {
                enabled: true,
                extract_model: Some("unavailable/aux".into()),
                ..devo_core::MemoryConfig::default()
            },
        )?);
        for _ in 0..2 {
            let context = crate::memory::scan::ScanContext {
                db: Arc::clone(&runtime.deps.db),
                model_context: runtime.deps.context_for_workspace(root.path()).await?,
                usage_ledger: runtime.usage_ledger.clone(),
                triggering_session: SessionId::new(),
                activity: Arc::new(IdleSources),
            };
            Arc::clone(&memory).run_background_scan(context).await?;
        }
        let connection = connect(&runtime).await?;
        let response = runtime
            .handle_incoming(
                connection,
                serde_json::json!({"id":2,"method":"memory/status","params":{}}),
            )
            .await
            .context("memory status")?;
        assert_eq!(
            response,
            serde_json::json!({
                "id":2,
                "result":{
                    "enabled":true,"storageHealth":"healthy",
                    "entryCount":0,"candidateCount":0,"pendingJobCount":0,
                    "retryingJobCount":0,"errorJobCount":1,
                    "lastSuccessfulScanAt":null,
                    "errorClasses":["credentials_unavailable"],
                }
            })
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        let ping = runtime
            .handle_incoming(
                connection,
                serde_json::json!({"id":3,"method":"runtime/ping","params":{}}),
            )
            .await
            .context("ping")?;
        assert!(ping.get("result").is_some());
    }
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-6
/// Verifies: unsafe assignment variants make no auxiliary provider request and safe token-count prose still extracts.
#[tokio::test]
async fn scan_credential_assignment_variants_never_send_source() -> Result<()> {
    let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 5)?;
    let source = std::fs::read_dir(root.path().join("sessions"))?
        .next()
        .unwrap()?
        .path();
    let original = std::fs::read_to_string(&source)?;
    for text in [
        "API key: \" \"",
        "password=;",
        "_API_KEY=ab",
        "_password=ab",
        "API key: ab",
        "API key = abcdefghijklmnop",
        "Credentials: API key: ab",
        "option = password=ab",
        "API\nkey=ab",
        "API\u{2003}key=ab",
        "API key:\nab",
        "API key:\u{2003}ab",
    ] {
        let quoted = serde_json::to_string(text)?;
        let escaped = &quoted[1..quoted.len() - 1];
        std::fs::write(&source, original.replace("I prefer tabs", escaped))?;
        scan(&runtime, root.path()).await?;
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
        assert_eq!(provider.requests.lock().unwrap().len(), 0);
    }
    std::fs::write(&source, original.replace("I prefer tabs", "token count: 5"))?;
    scan(&runtime, root.path()).await?;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests.lock().unwrap().len(), 1);
    Ok(())
}
