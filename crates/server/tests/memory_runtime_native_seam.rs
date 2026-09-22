use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use devo_core::AgentsMdConfig;
use devo_core::AppConfigStore;
use devo_core::BundledSkillsConfig;
use devo_core::FileSystemSkillCatalog;
use devo_core::PresetModelCatalog;
use devo_core::ProviderVendorCatalog;
use devo_core::SessionId;
use devo_core::SkillsConfig;
use devo_core::tools::ToolRegistry;
use devo_protocol::Model;
use devo_protocol::ModelRequest;
use devo_protocol::ModelResponse;
use devo_protocol::ProtocolErrorCode;
use devo_protocol::ResponseContent;
use devo_protocol::ResponseMetadata;
use devo_protocol::StopReason;
use devo_protocol::StreamEvent;
use devo_protocol::Usage;
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::MemoryEntry;
use devo_provider::ModelProviderSDK;
use devo_provider::SingleProviderRouter;
use devo_server::ClientTransportKind;
use devo_server::ServerRuntime;
use devo_server::ServerRuntimeDependencies;
use futures::Stream;
use futures::stream;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

struct NoopProvider;

struct BlockingProvider {
    release: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl ModelProviderSDK for NoopProvider {
    async fn completion(&self, _request: ModelRequest) -> Result<ModelResponse> {
        Ok(ModelResponse {
            id: "memory-native-seam-test-response".into(),
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
        "memory-native-seam-test-provider"
    }
}

#[async_trait::async_trait]
impl ModelProviderSDK for BlockingProvider {
    async fn completion(&self, _request: ModelRequest) -> Result<ModelResponse> {
        self.release.notified().await;
        Ok(ModelResponse {
            id: "memory-native-seam-blocking-response".into(),
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
        self.release.notified().await;
        Ok(Box::pin(stream::empty()))
    }

    fn name(&self) -> &str {
        "memory-native-seam-blocking-provider"
    }
}

fn build_test_runtime(data_root: &Path) -> Result<Arc<ServerRuntime>> {
    let provider: Arc<dyn ModelProviderSDK> = Arc::new(NoopProvider);
    build_test_runtime_with_provider(data_root, provider)
}

fn build_test_runtime_with_provider(
    data_root: &Path,
    provider: Arc<dyn ModelProviderSDK>,
) -> Result<Arc<ServerRuntime>> {
    let config_store = Arc::new(std::sync::Mutex::new(AppConfigStore::load(
        data_root.to_path_buf(),
        /*workspace_root*/ Some(data_root),
    )?));
    let db = Arc::new(devo_server::db::Database::open(data_root.join("devo.db"))?);
    Ok(ServerRuntime::new(
        data_root.join("server"),
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
    ))
}

async fn initialize_native_connection(
    runtime: &Arc<ServerRuntime>,
    connection_id: u64,
) -> Result<()> {
    let response = runtime
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
    anyhow::ensure!(
        response.get("result").is_some(),
        "Native initialize failed: {response}"
    );
    Ok(())
}

async fn create_native_session_subscription(
    runtime: &Arc<ServerRuntime>,
    connection_id: u64,
    session_id: &str,
) -> Result<()> {
    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 3,
                "method": "subscription/create",
                "params": {
                    "selectors": [{ "kind": "session", "sessionId": session_id }],
                    "includeSnapshot": false
                }
            }),
        )
        .await
        .expect("Native subscription/create response");
    anyhow::ensure!(
        response.get("result").is_some(),
        "Native subscription/create failed: {response}"
    );
    Ok(())
}

async fn start_native_session(
    runtime: &Arc<ServerRuntime>,
    connection_id: u64,
    cwd: &Path,
) -> Result<String> {
    initialize_native_connection(runtime, connection_id).await?;
    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 2,
                "method": "session/start",
                "params": {
                    "cwd": cwd,
                    "ephemeral": false,
                    "title": "memory runtime seam test",
                    "model": "test-model"
                }
            }),
        )
        .await
        .expect("Native session/start response");
    Ok(
        serde_json::from_value::<devo_server::SuccessResponse<devo_server::SessionStartResult>>(
            response,
        )?
        .result
        .session
        .session_id
        .to_string(),
    )
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-2, DD-13
/// Verifies: Native Project commands apply the global disable gate before selectors.
#[tokio::test]
async fn disabled_native_project_commands_do_not_require_a_session_selector() -> Result<()> {
    let data_root = TempDir::new()?;
    let runtime = build_test_runtime(data_root.path())?;
    let (outbound_tx, _outbound_rx) = devo_server::test_outbound_channel(/*capacity*/ 8);
    let connection_id = runtime
        .register_connection(ClientTransportKind::Stdio, outbound_tx)
        .await;
    initialize_native_connection(&runtime, connection_id).await?;

    let listed = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 2,
                "method": "memory/list",
                "params": { "scope": "project" }
            }),
        )
        .await
        .expect("disabled Project memory/list response");
    let listed = serde_json::from_value::<devo_server::SuccessResponse<Page<MemoryEntry>>>(listed)?;
    assert_eq!(
        listed.result,
        Page {
            data: Vec::new(),
            next_cursor: None,
        }
    );

    create_native_session_subscription(&runtime, connection_id, &SessionId::new().to_string())
        .await?;
    let listed_with_unavailable_selector = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 4,
                "method": "memory/list",
                "params": { "scope": "project" }
            }),
        )
        .await
        .expect("disabled Project memory/list response with unavailable selector");
    let listed_with_unavailable_selector = serde_json::from_value::<
        devo_server::SuccessResponse<Page<MemoryEntry>>,
    >(listed_with_unavailable_selector)?;
    assert_eq!(
        listed_with_unavailable_selector.result,
        Page {
            data: Vec::new(),
            next_cursor: None,
        }
    );

    let remembered = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 5,
                "method": "memory/remember",
                "params": { "text": "disabled Project memory", "scope": "project" }
            }),
        )
        .await
        .expect("disabled Project memory/remember response");
    let remembered = serde_json::from_value::<devo_protocol::ErrorResponse>(remembered)?;
    assert_eq!(remembered.error.code, ProtocolErrorCode::InternalError);
    assert_eq!(remembered.error.message, "memory is disabled");
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-3, DD-13
/// Verifies: missing and unavailable Native Session selectors fail deterministically.
#[tokio::test]
async fn native_project_commands_reject_missing_and_unavailable_session_selectors() -> Result<()> {
    let data_root = TempDir::new()?;
    std::fs::create_dir_all(data_root.path().join(".devo"))?;
    std::fs::write(
        data_root.path().join(".devo").join("config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    let runtime = build_test_runtime(data_root.path())?;
    let (outbound_tx, _outbound_rx) = devo_server::test_outbound_channel(/*capacity*/ 8);
    let connection_id = runtime
        .register_connection(ClientTransportKind::Stdio, outbound_tx)
        .await;
    initialize_native_connection(&runtime, connection_id).await?;

    let missing = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 2,
                "method": "memory/list",
                "params": { "scope": "project" }
            }),
        )
        .await
        .expect("missing selector response");
    let missing = serde_json::from_value::<devo_protocol::ErrorResponse>(missing)?;
    assert_eq!(missing.error.code, ProtocolErrorCode::InvalidParams);
    assert_eq!(
        missing.error.message,
        "memory/list Project scope requires a session-bound connection"
    );

    create_native_session_subscription(&runtime, connection_id, &SessionId::new().to_string())
        .await?;
    let unavailable = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 4,
                "method": "memory/list",
                "params": { "scope": "project" }
            }),
        )
        .await
        .expect("unavailable selector response");
    let unavailable = serde_json::from_value::<devo_protocol::ErrorResponse>(unavailable)?;
    assert_eq!(unavailable.error.code, ProtocolErrorCode::InvalidParams);
    assert_eq!(
        unavailable.error.message,
        "memory/list Project scope requires a session with a workspace root"
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-3, DD-13
/// Verifies: an unavailable Project identity returns a stable Native error
/// without exposing the selected workspace path.
#[tokio::test]
async fn native_project_commands_hide_unavailable_workspace_identity() -> Result<()> {
    let data_root = TempDir::new()?;
    let workspace_root = data_root.path().join("workspace-that-will-disappear");
    std::fs::create_dir_all(&workspace_root)?;
    std::fs::create_dir_all(data_root.path().join(".devo"))?;
    std::fs::write(
        data_root.path().join(".devo").join("config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    let runtime = build_test_runtime(data_root.path())?;
    let (outbound_tx, _outbound_rx) = devo_server::test_outbound_channel(/*capacity*/ 8);
    let connection_id = runtime
        .register_connection(ClientTransportKind::Stdio, outbound_tx)
        .await;
    let session_id = start_native_session(&runtime, connection_id, &workspace_root).await?;
    create_native_session_subscription(&runtime, connection_id, &session_id).await?;
    std::fs::remove_dir_all(&workspace_root)?;

    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 4,
                "method": "memory/list",
                "params": { "scope": "project" }
            }),
        )
        .await
        .expect("unavailable identity response");
    let response = serde_json::from_value::<devo_protocol::ErrorResponse>(response)?;
    assert_eq!(response.error.code, ProtocolErrorCode::InvalidParams);
    assert_eq!(
        response.error.message,
        "memory/list Project scope identity is unavailable"
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-3, DD-13
/// Verifies: a real active turn keeps complete provenance while a linked-worktree
/// selector resolves to the same canonical Project.
#[tokio::test]
async fn native_project_remember_preserves_active_turn_provenance_across_worktrees() -> Result<()> {
    let data_root = TempDir::new()?;
    let repository_root = data_root.path().join("repository");
    let common_git_dir = repository_root.join(".git");
    let linked_root = data_root.path().join("linked-worktree");
    let linked_git_dir = common_git_dir.join("worktrees").join("linked");
    std::fs::create_dir_all(&common_git_dir)?;
    std::fs::create_dir_all(&linked_git_dir)?;
    std::fs::create_dir_all(&linked_root)?;
    let common_dir = Path::new("..").join("..");
    std::fs::write(
        linked_git_dir.join("commondir"),
        format!("{}\n", common_dir.display()),
    )?;
    std::fs::write(
        linked_root.join(".git"),
        format!("gitdir: {}\n", linked_git_dir.display()),
    )?;
    std::fs::create_dir_all(data_root.path().join(".devo"))?;
    std::fs::write(
        data_root.path().join(".devo").join("config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    let release = Arc::new(tokio::sync::Notify::new());
    let provider: Arc<dyn ModelProviderSDK> = Arc::new(BlockingProvider {
        release: Arc::clone(&release),
    });
    let runtime = build_test_runtime_with_provider(data_root.path(), provider)?;
    let (active_tx, mut active_notifications) =
        devo_server::test_outbound_channel(/*capacity*/ 16);
    let active_connection = runtime
        .register_connection(ClientTransportKind::Stdio, active_tx)
        .await;
    let (linked_tx, _linked_rx) = devo_server::test_outbound_channel(/*capacity*/ 8);
    let linked_connection = runtime
        .register_connection(ClientTransportKind::Stdio, linked_tx)
        .await;
    let active_session =
        start_native_session(&runtime, active_connection, &repository_root).await?;
    let linked_session = start_native_session(&runtime, linked_connection, &linked_root).await?;
    create_native_session_subscription(&runtime, active_connection, &linked_session).await?;

    let turn_start = runtime
        .handle_incoming(
            active_connection,
            serde_json::json!({
                "id": 4,
                "method": "turn/start",
                "params": {
                    "sessionId": active_session,
                    "input": [{ "type": "text", "text": "remember this project fact" }],
                    "idempotencyKey": "active-project-provenance"
                }
            }),
        )
        .await
        .expect("active turn/start response");
    let turn = serde_json::from_value::<
        devo_server::SuccessResponse<devo_protocol::native::rpc_turn::TurnStartResult>,
    >(turn_start)?
    .result
    .turn;
    let source_user_item_id = loop {
        let notification = tokio::time::timeout(
            Duration::from_secs(/*seconds*/ 2),
            active_notifications.recv(),
        )
        .await?
        .context("active turn notification channel closed")?;
        if notification["method"] == "item/started"
            && notification["params"]["item"]["sessionId"] == active_session
        {
            break notification["params"]["item"]["id"]
                .as_str()
                .context("user item id in item/started")?
                .to_string();
        }
    };

    let remembered = runtime
        .handle_incoming(
            active_connection,
            serde_json::json!({
                "id": 5,
                "method": "memory/remember",
                "params": {
                    "text": "the project uses Rust",
                    "scope": "project",
                    "sourceUserItemId": source_user_item_id
                }
            }),
        )
        .await
        .expect("active Project memory/remember response");
    let remembered = serde_json::from_value::<MemoryEntry>(remembered["result"].clone())?;
    assert_eq!(
        remembered.provenance,
        vec![devo_protocol::native::rpc_memory::MemoryProvenance {
            source_session_id: Some(active_session),
            source_turn_id: Some(turn.id.to_string()),
            source_user_item_id: Some(source_user_item_id.into()),
        }]
    );
    release.notify_waiters();
    Ok(())
}
