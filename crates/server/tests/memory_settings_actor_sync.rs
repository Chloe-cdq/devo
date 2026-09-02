use std::pin::Pin;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use async_trait::async_trait;
use devo_core::AgentsMdConfig;
use devo_core::AppConfigStore;
use devo_core::BundledSkillsConfig;
use devo_core::FileSystemSkillCatalog;
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
use devo_protocol::native::model::PermissionProfile;
use devo_protocol::native::session::MemorySetting;
use devo_protocol::native::session::SessionSettings;
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

#[async_trait]
impl ModelProviderSDK for NoopProvider {
    async fn completion(&self, _request: ModelRequest) -> Result<ModelResponse> {
        Ok(ModelResponse {
            id: "memory-settings-test-response".into(),
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
        "memory-settings-test-provider"
    }
}

/// Verifies: a persisted memory patch reaches actor state before the next actor command.
#[tokio::test]
async fn durable_memory_settings_are_inherited_by_the_next_fork() -> Result<()> {
    let data_root = TempDir::new()?;
    let runtime = build_runtime(data_root.path())?;
    let connection_id = initialize_connection(&runtime).await?;
    let started = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 1,
                "method": "session/start",
                "params": {
                    "cwd": data_root.path(),
                    "ephemeral": false,
                    "title": "Memory actor sync",
                    "model": "test-model"
                }
            }),
        )
        .await
        .context("session/start response")?;
    let session_id = serde_json::from_value::<
        devo_server::SuccessResponse<devo_server::SessionStartResult>,
    >(started)?
    .result
    .session
    .session_id;

    let updated = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 2,
                "method": "session/metadata/update",
                "params": {
                    "sessionId": session_id,
                    "expectedVersion": 0,
                    "settings": {
                        "memoryRecall": "off",
                        "memoryContribution": "on"
                    }
                }
            }),
        )
        .await
        .context("session/metadata/update response")?;
    let _: devo_server::SuccessResponse<
        devo_protocol::native::rpc_session::SessionMetadataUpdateResult,
    > = serde_json::from_value(updated)?;

    let forked = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 3,
                "method": "session/fork",
                "params": { "sessionId": session_id }
            }),
        )
        .await
        .context("session/fork response")?;
    let forked = serde_json::from_value::<
        devo_server::SuccessResponse<devo_protocol::native::rpc_session::SessionForkResult>,
    >(forked)?
    .result;

    assert_eq!(
        forked.session.settings,
        SessionSettings {
            permission_profile: PermissionProfile::Default,
            reasoning_effort: None,
            mode: None,
            sandbox_profile: Some("workspace-write".to_string()),
            effective_context_window: None,
            memory_recall: MemorySetting::Off,
            memory_contribution: MemorySetting::On,
        }
    );
    Ok(())
}

fn build_runtime(data_root: &std::path::Path) -> Result<Arc<ServerRuntime>> {
    let provider: Arc<dyn ModelProviderSDK> = Arc::new(NoopProvider);
    let db = Arc::new(devo_server::db::Database::open(
        data_root.join("memory-settings-actor-sync.db"),
    )?);
    Ok(ServerRuntime::new(
        data_root.to_path_buf(),
        ServerRuntimeDependencies::new(
            Arc::clone(&provider),
            Arc::new(SingleProviderRouter::new(provider)),
            Arc::new(ToolRegistry::new()),
            devo_server::empty_mcp_manager(),
            "test-model".to_string(),
            Arc::new(PresetModelCatalog::new(vec![Model {
                slug: "test-model".to_string(),
                display_name: "test-model".to_string(),
                ..Model::default()
            }])),
            Arc::new(ProviderVendorCatalog::default()),
            Box::new(FileSystemSkillCatalog::new(SkillsConfig {
                bundled: Some(BundledSkillsConfig { enabled: false }),
                ..SkillsConfig::default()
            })),
            AgentsMdConfig::default(),
            db,
            Arc::new(std::sync::Mutex::new(AppConfigStore::load(
                data_root.to_path_buf(),
                None,
            )?)),
        ),
    ))
}

async fn initialize_connection(runtime: &Arc<ServerRuntime>) -> Result<u64> {
    let (notifications_tx, _notifications_rx) = devo_server::test_outbound_channel(64);
    let connection_id = runtime
        .register_connection(ClientTransportKind::Stdio, notifications_tx)
        .await;
    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 0,
                "method": "initialize",
                "params": {
                    "protocolVersion": 1,
                    "clientCapabilities": {},
                    "_meta": { "devo": { "protocol": "native" } },
                    "clientInfo": {
                        "name": "test",
                        "title": "test",
                        "version": "1.0.0"
                    }
                }
            }),
        )
        .await
        .context("initialize response")?;
    assert_eq!(
        response["result"]["agentInfo"]["name"],
        serde_json::json!("devo-server")
    );
    Ok(connection_id)
}
