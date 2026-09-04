use std::fs;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
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
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::{MemoryKind, MemoryScope};
use devo_provider::ModelProviderSDK;
use devo_provider::SingleProviderRouter;
use devo_server::ClientTransportKind;
use devo_server::ServerRuntime;
use devo_server::ServerRuntimeDependencies;
use devo_server::memory::{
    ListMemoryRequest, MemoryCommand, MemoryCommandResult, MemoryError, MemoryRememberRequest,
    MemoryRuntime, PrepareMemoryRequest,
};
use futures::Stream;
use futures::stream;
use pretty_assertions::assert_eq;
use rusqlite::Connection;
use tempfile::TempDir;

struct NoopProvider;

#[async_trait::async_trait]
impl ModelProviderSDK for NoopProvider {
    async fn completion(&self, _request: ModelRequest) -> Result<ModelResponse> {
        Ok(ModelResponse {
            id: "memory-native-test-response".into(),
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
        "memory-native-test-provider"
    }
}

fn remember_request(
    text: &str,
    source_user_item_id: &str,
    workspace_root: &std::path::Path,
) -> MemoryRememberRequest {
    MemoryRememberRequest {
        text: text.to_string(),
        scope: MemoryScope::User,
        kind: None,
        source_user_item_id: source_user_item_id.to_string(),
        source_session_id: "ses-1".to_string(),
        source_turn_id: Some("turn-1".to_string()),
        workspace_root: workspace_root.to_path_buf(),
    }
}

#[tokio::test]
async fn explicit_user_memory_is_committed_and_deduplicated() {
    let data_root = TempDir::new().expect("memory data root");
    let runtime = MemoryRuntime::open(
        data_root.path().join("memory"),
        MemoryConfig {
            enabled: true,
            ..MemoryConfig::default()
        },
    )
    .expect("open enabled memory runtime");

    let request = MemoryRememberRequest {
        kind: Some(MemoryKind::Preference),
        ..remember_request("I prefer dark mode", "item-user-1", data_root.path())
    };
    let first = runtime
        .execute_command(MemoryCommand::Remember(request.clone()))
        .await
        .expect("commit explicit memory");
    let first = match first {
        MemoryCommandResult::Remember(entry) => entry,
        other => panic!("unexpected remember result: {other:?}"),
    };

    let second = runtime
        .execute_command(MemoryCommand::Remember(MemoryRememberRequest {
            text: "  I prefer   dark mode ".to_string(),
            ..request
        }))
        .await
        .expect("deduplicate explicit memory");
    let second = match second {
        MemoryCommandResult::Remember(entry) => entry,
        other => panic!("unexpected remember result: {other:?}"),
    };

    assert_eq!(second.entry_id, first.entry_id);
    assert_eq!(second.body, first.body);

    let listed = runtime
        .execute_command(MemoryCommand::List(ListMemoryRequest {
            scope: Some(MemoryScope::User),
            workspace_root: data_root.path().to_path_buf(),
            ..ListMemoryRequest::default()
        }))
        .await
        .expect("list explicit memory");
    let listed: Page<_> = match listed {
        MemoryCommandResult::List(page) => page,
        other => panic!("unexpected list result: {other:?}"),
    };

    assert_eq!(listed.data, vec![second]);
    assert_eq!(listed.next_cursor, None);

    let connection = Connection::open(data_root.path().join("memory/memory.sqlite3"))
        .expect("open memory database");
    let evidence_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM memory_evidence", [], |row| row.get(0))
        .expect("count evidence");
    assert_eq!(evidence_count, 1);
}

#[tokio::test]
async fn secret_memory_is_rejected_before_sqlite_fts_and_projection() {
    let data_root = TempDir::new().expect("memory data root");
    let memory_root = data_root.path().join("memory");
    let runtime = MemoryRuntime::open(
        memory_root.clone(),
        MemoryConfig {
            enabled: true,
            ..MemoryConfig::default()
        },
    )
    .expect("open enabled memory runtime");

    let error = runtime
        .execute_command(MemoryCommand::Remember(remember_request(
            "use api_key=super-secret-value",
            "item-secret",
            data_root.path(),
        )))
        .await
        .expect_err("secret memory must be rejected");
    assert_eq!(
        error.to_string(),
        MemoryError::SecretContentRejected.to_string()
    );
    for (index, secret) in [
        "password=super-secret-value",
        "token: abcdefghijk",
        "AKIAIOSFODNN7EXAMPLE",
    ]
    .iter()
    .enumerate()
    {
        runtime
            .execute_command(MemoryCommand::Remember(remember_request(
                secret,
                &format!("item-secret-{index}"),
                data_root.path(),
            )))
            .await
            .expect_err("detected secret must be rejected");
    }

    let connection = Connection::open(memory_root.join("memory.sqlite3")).expect("open database");
    let entry_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM memory_entries", [], |row| row.get(0))
        .expect("count entries");
    let fts_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM memory_entries_fts", [], |row| {
            row.get(0)
        })
        .expect("count fts entries");
    assert_eq!(entry_count, 0);
    assert_eq!(fts_count, 0);
    assert!(!memory_root.join("user").join("MEMORY.md").exists());
}

#[tokio::test]
async fn user_memory_listing_is_paginated_and_projection_is_regenerated() {
    let data_root = TempDir::new().expect("memory data root");
    let memory_root = data_root.path().join("memory");
    let runtime = MemoryRuntime::open(
        memory_root.clone(),
        MemoryConfig {
            enabled: true,
            ..MemoryConfig::default()
        },
    )
    .expect("open enabled memory runtime");

    for (index, text) in ["alpha fact", "beta fact", "gamma fact"]
        .into_iter()
        .enumerate()
    {
        runtime
            .execute_command(MemoryCommand::Remember(remember_request(
                text,
                &format!("item-{index}"),
                data_root.path(),
            )))
            .await
            .expect("commit memory");
    }

    let projection_path = memory_root.join("user").join("MEMORY.md");
    fs::write(&projection_path, "manual content must not be canonical").expect("edit projection");
    runtime
        .execute_command(MemoryCommand::Remember(remember_request(
            "delta fact",
            "item-delta",
            data_root.path(),
        )))
        .await
        .expect("regenerate projection");
    let projection = fs::read_to_string(&projection_path).expect("read projection");
    assert!(projection.contains("delta fact"));
    assert!(!projection.contains("manual content"));
    assert!(projection.contains("Read-only"));

    let first_page = runtime
        .execute_command(MemoryCommand::List(ListMemoryRequest {
            scope: Some(MemoryScope::User),
            limit: Some(2),
            workspace_root: data_root.path().to_path_buf(),
            ..ListMemoryRequest::default()
        }))
        .await
        .expect("list first page");
    let first_page: Page<_> = match first_page {
        MemoryCommandResult::List(page) => page,
        other => panic!("unexpected list result: {other:?}"),
    };
    assert_eq!(first_page.data.len(), 2);
    assert!(first_page.next_cursor.is_some());

    let second_page = runtime
        .execute_command(MemoryCommand::List(ListMemoryRequest {
            scope: Some(MemoryScope::User),
            cursor: first_page.next_cursor,
            limit: Some(2),
            workspace_root: data_root.path().to_path_buf(),
            ..ListMemoryRequest::default()
        }))
        .await
        .expect("list second page");
    let second_page: Page<_> = match second_page {
        MemoryCommandResult::List(page) => page,
        other => panic!("unexpected list result: {other:?}"),
    };
    assert_eq!(second_page.data.len(), 2);
    assert_eq!(second_page.next_cursor, None);
}

#[tokio::test]
async fn native_memory_remember_and_list_use_the_user_scope() -> Result<()> {
    let data_root = TempDir::new()?;
    fs::create_dir_all(data_root.path().join(".devo"))?;
    fs::write(
        data_root.path().join(".devo/config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    let config_store = Arc::new(std::sync::Mutex::new(AppConfigStore::load(
        data_root.path().to_path_buf(),
        Some(data_root.path()),
    )?));
    let provider: Arc<dyn ModelProviderSDK> = Arc::new(NoopProvider);
    let db = Arc::new(devo_server::db::Database::open(
        data_root.path().join("devo.db"),
    )?);
    let runtime = ServerRuntime::new(
        data_root.path().join("server"),
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
    runtime
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
    let session_started = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 2,
                "method": "session/start",
                "params": {
                    "cwd": data_root.path(),
                    "ephemeral": false,
                    "title": "memory test",
                    "model": "test-model"
                }
            }),
        )
        .await
        .expect("Native session/start response");
    let session_id = serde_json::from_value::<
        devo_server::SuccessResponse<devo_server::SessionStartResult>,
    >(session_started)?
    .result
    .session
    .session_id
    .to_string();

    let remembered = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 3,
                "method": "memory/remember",
                "params": {
                    "text": "I prefer dark mode",
                    "sourceUserItemId": "item-user-1"
                }
            }),
        )
        .await
        .expect("memory/remember response");
    let remembered: devo_protocol::native::rpc_memory::MemoryEntry =
        serde_json::from_value(remembered["result"].clone())?;
    assert!(remembered.entry_id.as_str().starts_with("mem_"));
    assert_eq!(remembered.scope, MemoryScope::User);
    assert_eq!(remembered.kind, MemoryKind::Preference);
    assert_eq!(
        remembered.origin,
        devo_protocol::native::rpc_memory::MemoryOrigin::ExplicitUser
    );
    assert_eq!(
        remembered
            .provenance
            .first()
            .and_then(|provenance| provenance.source_session_id.as_deref()),
        Some(session_id.as_str())
    );

    let listed = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 4,
                "method": "memory/list",
                "params": { "scope": "user" }
            }),
        )
        .await
        .expect("memory/list response");
    let listed: Page<devo_protocol::native::rpc_memory::MemoryEntry> =
        serde_json::from_value(listed["result"].clone())?;
    assert_eq!(listed.data, vec![remembered]);
    Ok(())
}

#[tokio::test]
async fn committed_memory_only_enters_a_new_prepared_turn_snapshot() {
    let data_root = TempDir::new().expect("memory data root");
    let runtime = MemoryRuntime::open(
        data_root.path().join("memory"),
        MemoryConfig {
            enabled: true,
            ..MemoryConfig::default()
        },
    )
    .expect("open enabled memory runtime");
    let request = PrepareMemoryRequest {
        workspace_root: data_root.path().to_path_buf(),
    };

    let before = runtime
        .prepare_turn(request.clone())
        .await
        .expect("prepare initial snapshot");
    runtime
        .execute_command(MemoryCommand::Remember(remember_request(
            "remember this fact",
            "item-snapshot",
            data_root.path(),
        )))
        .await
        .expect("commit memory");
    let after = runtime
        .prepare_turn(request)
        .await
        .expect("prepare next snapshot");

    assert!(before.user_entries.is_empty());
    assert_eq!(after.user_entries.len(), 1);
}
