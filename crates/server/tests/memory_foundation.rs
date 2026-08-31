use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use devo_core::AgentsMdConfig;
use devo_core::AppConfigStore;
use devo_core::BundledSkillsConfig;
use devo_core::FileSystemSkillCatalog;
use devo_core::MemoryConfig;
use devo_core::PresetModelCatalog;
use devo_core::ProviderVendorCatalog;
use devo_core::SkillsConfig;
use devo_core::tools::ToolRegistry;
use devo_protocol::Model;
use devo_protocol::ModelRequest;
use devo_protocol::ModelResponse;
use devo_protocol::ResponseContent;
use devo_protocol::ResponseMetadata;
use devo_protocol::StopReason;
use devo_protocol::StreamEvent;
use devo_protocol::Usage;
use devo_protocol::native::rpc_memory::MemoryStatus;
use devo_protocol::native::session::MemorySetting;
use devo_provider::ModelProviderSDK;
use devo_provider::SingleProviderRouter;
use devo_server::ClientTransportKind;
use devo_server::ServerRuntime;
use devo_server::ServerRuntimeDependencies;
use devo_server::memory::{
    EnqueueOutcome, MemoryCommand, MemoryCommandResult, MemoryRuntime, PreparedMemory,
    SessionMemorySource,
};
use futures::Stream;
use futures::stream;
use pretty_assertions::assert_eq;
use rusqlite::Connection;
use tempfile::TempDir;

struct NoopProvider;

#[async_trait]
impl ModelProviderSDK for NoopProvider {
    async fn completion(&self, _request: ModelRequest) -> Result<ModelResponse> {
        Ok(ModelResponse {
            id: "memory-test-response".into(),
            content: vec![ResponseContent::Text("ok".into())],
            stop_reason: Some(StopReason::EndTurn),
            usage: Usage::default(),
            metadata: ResponseMetadata::default(),
        })
    }

    async fn completion_stream(
        &self,
        _request: ModelRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamEvent>> + Send>>> {
        Ok(Box::pin(stream::empty()))
    }

    fn name(&self) -> &str {
        "memory-test-provider"
    }
}

#[test]
fn memory_config_defaults_are_disabled_and_global_gate_wins() {
    let config = MemoryConfig::default();
    assert_eq!(
        config,
        MemoryConfig {
            enabled: false,
            default_recall: MemorySetting::On,
            default_contribution: MemorySetting::On,
            min_source_idle_hours: 6,
            source_window_days: 30,
            inferred_stale_after_days: 90,
            candidate_and_job_retention_days: 30,
            max_sources_per_scan: 2,
            max_entries_per_turn: 12,
            max_prompt_tokens: 2_000,
            min_rate_limit_remaining_percent: 25,
            extract_model: None,
        }
    );
    assert_eq!(config.effective_recall(), MemorySetting::Off);
    assert_eq!(config.effective_contribution(), MemorySetting::Off);

    let enabled = MemoryConfig {
        enabled: true,
        ..config
    };
    assert_eq!(enabled.effective_recall(), MemorySetting::On);
    assert_eq!(enabled.effective_contribution(), MemorySetting::On);
}

#[tokio::test]
async fn default_memory_runtime_is_disabled_and_schema_is_idempotent() {
    let data_root = TempDir::new().expect("memory data root");
    let memory_root = data_root.path().join("memory");
    let runtime = MemoryRuntime::open(memory_root.clone(), MemoryConfig::default())
        .expect("open disabled memory runtime");

    let status = runtime
        .execute_command(MemoryCommand::Status)
        .await
        .expect("read memory status");
    assert_eq!(
        status,
        MemoryCommandResult::Status(MemoryStatus {
            enabled: false,
            storage_health: "healthy".into(),
            entry_count: 0,
            candidate_count: 0,
            pending_job_count: 0,
            retrying_job_count: 0,
            error_job_count: 0,
        })
    );
    assert_eq!(
        runtime
            .prepare_turn(Default::default())
            .await
            .expect("prepare disabled memory"),
        PreparedMemory::default()
    );
    assert_eq!(
        runtime
            .enqueue_source(SessionMemorySource::default())
            .await
            .expect("enqueue disabled memory source"),
        EnqueueOutcome { accepted: false }
    );

    drop(runtime);
    let second_runtime = MemoryRuntime::open(memory_root.clone(), MemoryConfig::default())
        .expect("reopen disabled memory runtime");
    drop(second_runtime);

    let connection = Connection::open(memory_root.join("memory.sqlite3")).expect("open schema");
    let tables: HashSet<String> = connection
        .prepare(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'virtual table') ORDER BY name",
        )
        .expect("prepare schema query")
        .query_map([], |row| row.get(0))
        .expect("query schema")
        .collect::<Result<_, _>>()
        .expect("collect schema names");

    assert!(tables.contains("memory_entries"));
    assert!(tables.contains("memory_candidates"));
    assert!(tables.contains("memory_evidence"));
    assert!(tables.contains("memory_revocations"));
    assert!(tables.contains("memory_jobs"));
    assert!(tables.contains("memory_scope_state"));
    assert!(tables.contains("memory_entries_fts"));

    let job_columns: HashSet<String> = connection
        .prepare("PRAGMA table_info(memory_jobs)")
        .expect("prepare memory_jobs schema query")
        .query_map([], |row| row.get(1))
        .expect("query memory_jobs schema")
        .collect::<Result<_, _>>()
        .expect("collect memory_jobs columns");
    assert!(job_columns.contains("job_kind"));
    assert!(job_columns.contains("job_key"));
    assert!(job_columns.contains("lease_owner"));
    assert!(job_columns.contains("claimed_at"));

    let schema_version: String = connection
        .query_row(
            "SELECT value FROM memory_schema_meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .expect("read memory schema version");
    assert_eq!(schema_version, "2");
}

#[test]
fn legacy_memory_jobs_schema_is_migrated() -> Result<()> {
    let data_root = TempDir::new()?;
    let memory_root = data_root.path().join("memory");
    std::fs::create_dir_all(&memory_root)?;
    let connection = Connection::open(memory_root.join("memory.sqlite3"))?;
    connection.execute_batch(
        "
        CREATE TABLE memory_schema_meta (
            key TEXT PRIMARY KEY NOT NULL,
            value TEXT NOT NULL
        );
        INSERT INTO memory_schema_meta (key, value)
        VALUES ('schema_version', '1');
        CREATE TABLE memory_jobs (
            job_id TEXT PRIMARY KEY NOT NULL,
            source_session_id TEXT NOT NULL,
            source_watermark TEXT NOT NULL,
            state TEXT NOT NULL,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            lease_until TEXT,
            retry_at TEXT,
            error_class TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(source_session_id, source_watermark)
        );
        INSERT INTO memory_jobs (
            job_id, source_session_id, source_watermark, state, created_at, updated_at
        ) VALUES ('job-1', 'session-1', 'watermark-1', 'pending', 'now', 'now');
        ",
    )?;
    drop(connection);

    let runtime = MemoryRuntime::open(memory_root.clone(), MemoryConfig::default())?;
    drop(runtime);

    let connection = Connection::open(memory_root.join("memory.sqlite3"))?;
    let job_columns: HashSet<String> = connection
        .prepare("PRAGMA table_info(memory_jobs)")?
        .query_map([], |row| row.get(1))?
        .collect::<Result<_, _>>()?;
    assert!(job_columns.contains("job_kind"));
    assert!(job_columns.contains("job_key"));
    assert!(job_columns.contains("lease_owner"));
    assert!(job_columns.contains("claimed_at"));
    let job_key: String = connection.query_row(
        "SELECT job_key FROM memory_jobs WHERE job_id = 'job-1'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(job_key, "session-1:watermark-1");
    let schema_version: String = connection.query_row(
        "SELECT value FROM memory_schema_meta WHERE key = 'schema_version'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(schema_version, "2");
    Ok(())
}

#[tokio::test]
async fn native_memory_status_reports_disabled_runtime() -> Result<()> {
    let data_root = TempDir::new()?;
    let provider: Arc<dyn ModelProviderSDK> = Arc::new(NoopProvider);
    let db = Arc::new(devo_server::db::Database::open(
        data_root.path().join("devo.db"),
    )?);
    let config_store = Arc::new(std::sync::Mutex::new(AppConfigStore::load(
        data_root.path().to_path_buf(),
        None,
    )?));
    let runtime = ServerRuntime::new(
        data_root.path().to_path_buf(),
        ServerRuntimeDependencies::new(
            Arc::clone(&provider),
            Arc::new(SingleProviderRouter::new(Arc::clone(&provider))),
            Arc::new(ToolRegistry::new()),
            devo_server::empty_mcp_manager(),
            "test-model".into(),
            Arc::new(PresetModelCatalog::new(vec![Model {
                slug: "test-model".into(),
                display_name: "test-model".into(),
                ..Model::default()
            }])),
            Arc::new(ProviderVendorCatalog::default()),
            Box::new(FileSystemSkillCatalog::new(SkillsConfig {
                bundled: Some(BundledSkillsConfig { enabled: false }),
                ..SkillsConfig::default()
            })),
            AgentsMdConfig::default(),
            db,
            config_store,
        ),
    );
    let (notifications_tx, _notifications_rx) = devo_server::test_outbound_channel(8);
    let connection_id = runtime
        .register_connection(ClientTransportKind::Stdio, notifications_tx)
        .await;
    let initialize = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": 1,
                    "clientCapabilities": {},
                    "_meta": { "devo": { "protocol": "native" } }
                }
            }),
        )
        .await
        .expect("Native initialize response");
    assert!(initialize.get("result").is_some());

    let status = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 2,
                "method": "memory/status",
                "params": {}
            }),
        )
        .await
        .expect("memory/status response");
    assert_eq!(
        status["result"],
        serde_json::json!({
            "enabled": false,
            "storageHealth": "healthy",
            "entryCount": 0,
            "candidateCount": 0,
            "pendingJobCount": 0,
            "retryingJobCount": 0,
            "errorJobCount": 0
        })
    );
    Ok(())
}
