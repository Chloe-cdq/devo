use std::fs;
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
use devo_core::MemoryConfig;
use devo_core::PresetModelCatalog;
use devo_core::ProviderVendorCatalog;
use devo_core::SkillsConfig;
use devo_core::tools::ToolRegistry;
use devo_protocol::Model;
use devo_protocol::ModelRequest;
use devo_protocol::ModelResponse;
use devo_protocol::ProtocolErrorCode;
use devo_protocol::ResponseContent;
use devo_protocol::ResponseMetadata;
use devo_protocol::SessionId;
use devo_protocol::StopReason;
use devo_protocol::StreamEvent;
use devo_protocol::TurnId;
use devo_protocol::Usage;
use devo_protocol::native::ids::ItemId;
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::{MemoryForgetResult, MemoryKind, MemoryScope, MemoryState};
use devo_provider::ModelProviderSDK;
use devo_provider::SingleProviderRouter;
use devo_server::ClientTransportKind;
use devo_server::ServerRuntime;
use devo_server::ServerRuntimeDependencies;
use devo_server::memory::{
    ListMemoryRequest, MemoryCommand, MemoryCommandResult, MemoryError, MemoryRememberRequest,
    MemoryRuntime, MemorySourceContext, PrepareMemoryRequest,
};
use futures::Stream;
use futures::stream;
use pretty_assertions::assert_eq;
use rusqlite::Connection;
use tempfile::TempDir;
use uuid::Uuid;

struct NoopProvider;

struct BlockingProvider {
    release: Arc<tokio::sync::Notify>,
}

fn test_uuid(seed: &str) -> Uuid {
    let value = seed.bytes().fold(0_u128, |value, byte| {
        value.rotate_left(5) ^ u128::from(byte)
    });
    Uuid::from_u128(value)
}

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

#[async_trait::async_trait]
impl ModelProviderSDK for BlockingProvider {
    async fn completion(&self, _request: ModelRequest) -> Result<ModelResponse> {
        self.release.notified().await;
        Ok(ModelResponse {
            id: "memory-blocking-test-response".into(),
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
        "memory-blocking-test-provider"
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
        source: MemorySourceContext {
            user_item_id: Some(ItemId::from_string(format!(
                "item_{:032x}",
                test_uuid(source_user_item_id).as_u128()
            ))),
            session_id: SessionId::from(test_uuid("ses-1")),
            turn_id: Some(TurnId::from(test_uuid("turn-1"))),
            workspace_root: workspace_root.to_path_buf(),
        },
    }
}

fn build_memory_test_runtime(data_root: &Path) -> Result<Arc<ServerRuntime>> {
    let provider: Arc<dyn ModelProviderSDK> = Arc::new(NoopProvider);
    build_memory_test_runtime_with_provider(data_root, provider)
}

fn build_memory_test_runtime_with_provider(
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

async fn start_native_session(
    runtime: &Arc<ServerRuntime>,
    connection_id: u64,
    cwd: &Path,
) -> Result<String> {
    initialize_native_connection(runtime, connection_id).await?;
    start_native_session_after_initialize(runtime, connection_id, cwd, /*request_id*/ 2).await
}

async fn start_native_session_after_initialize(
    runtime: &Arc<ServerRuntime>,
    connection_id: u64,
    cwd: &Path,
    request_id: u64,
) -> Result<String> {
    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": request_id,
                "method": "session/start",
                "params": {
                    "cwd": cwd,
                    "ephemeral": false,
                    "title": "memory selector test",
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

async fn create_native_session_subscription(
    runtime: &Arc<ServerRuntime>,
    connection_id: u64,
    session_id: &str,
    request_id: u64,
) -> Result<String> {
    create_native_subscription(
        runtime,
        connection_id,
        serde_json::json!([{ "kind": "session", "sessionId": session_id }]),
        request_id,
    )
    .await
}

async fn create_native_subscription(
    runtime: &Arc<ServerRuntime>,
    connection_id: u64,
    selectors: serde_json::Value,
    request_id: u64,
) -> Result<String> {
    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": request_id,
                "method": "subscription/create",
                "params": {
                    "selectors": selectors,
                    "includeSnapshot": false
                }
            }),
        )
        .await
        .expect("Native subscription/create response");
    Ok(serde_json::from_value::<
        devo_server::SuccessResponse<devo_protocol::native::event::SubscriptionCreateResult>,
    >(response)?
    .result
    .subscription_id
    .to_string())
}

fn project_remember_request(
    text: &str,
    source_user_item_id: &str,
    workspace_root: &std::path::Path,
) -> MemoryRememberRequest {
    MemoryRememberRequest {
        scope: MemoryScope::Project,
        ..remember_request(text, source_user_item_id, workspace_root)
    }
}

/// Trace: L2-DES-MEM-001
/// Verifies: explicit User memory is committed and canonical duplicates are merged.
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
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_) => {
            panic!("unexpected remember result")
        }
    };

    let second = runtime
        .execute_command(MemoryCommand::Remember(MemoryRememberRequest {
            text: "I prefer dark mode.".to_string(),
            ..request
        }))
        .await
        .expect("deduplicate explicit memory");
    let second = match second {
        MemoryCommandResult::Remember(entry) => entry,
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_) => {
            panic!("unexpected remember result")
        }
    };

    assert_eq!(second.entry_id, first.entry_id);
    assert_eq!(second.body, "I prefer dark mode.");

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
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::Remember(_) => {
            panic!("unexpected list result")
        }
    };

    assert_eq!(listed.data, vec![second]);
    assert_eq!(listed.next_cursor, None);

    let connection = Connection::open(data_root.path().join("memory").join("memory.sqlite3"))
        .expect("open memory database");
    let evidence_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM memory_evidence", [], |row| row.get(0))
        .expect("count evidence");
    assert_eq!(evidence_count, 1);
}

/// Trace: L2-DES-MEM-001
/// Verifies: secret-bearing memory is rejected before SQLite, FTS, or projection writes.
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

/// Trace: L2-DES-MEM-001
/// Verifies: User listing pagination and atomic projection regeneration expose canonical entries.
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
    assert!(projection.contains("state: active"));
    assert!(projection.contains("origin: explicit_user"));
    assert!(projection.contains("created_at:"));
    assert!(projection.contains(&format!(
        "source_session_id: {}",
        SessionId::from(test_uuid("ses-1"))
    )));

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
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::Remember(_) => {
            panic!("unexpected list result")
        }
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
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::Remember(_) => {
            panic!("unexpected list result")
        }
    };
    assert_eq!(second_page.data.len(), 2);
    assert_eq!(second_page.next_cursor, None);
}

/// Trace: L2-DES-MEM-001
/// Verifies: Native remember/list support User and Project scopes and expose safe provenance.
#[tokio::test]
async fn native_memory_remember_and_list_support_user_and_project_scopes() -> Result<()> {
    let data_root = TempDir::new()?;
    fs::create_dir_all(data_root.path().join(".devo"))?;
    fs::write(
        data_root.path().join(".devo").join("config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    let runtime = build_memory_test_runtime(data_root.path())?;
    let (notifications_tx, _notifications_rx) =
        devo_server::test_outbound_channel(/*capacity*/ 8);
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
    let _subscription = create_native_session_subscription(
        &runtime,
        connection_id,
        &session_id,
        /*request_id*/ 6,
    )
    .await?;

    let rejected = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 3,
                "method": "memory/remember",
                "params": {
                    "text": "I prefer dark mode",
                    "sourceUserItemId": "item-from-another-turn"
                }
            }),
        )
        .await
        .expect("memory/remember must reject an unbound source item");
    let rejected: devo_protocol::ErrorResponse = serde_json::from_value(rejected)?;
    assert_eq!(rejected.error.code, ProtocolErrorCode::InvalidParams);
    assert_eq!(
        rejected.error.message,
        "direct memory/remember commands must omit sourceUserItemId"
    );

    let remembered = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 4,
                "method": "memory/remember",
                "params": {
                    "text": "I prefer dark mode"
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
    assert_eq!(
        remembered
            .provenance
            .first()
            .and_then(|provenance| provenance.source_user_item_id.as_ref()),
        None
    );

    let listed = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 5,
                "method": "memory/list",
                "params": { "scope": "user" }
            }),
        )
        .await
        .expect("memory/list response");
    let listed: Page<devo_protocol::native::rpc_memory::MemoryEntry> =
        serde_json::from_value(listed["result"].clone())?;
    assert_eq!(listed.data, vec![remembered]);

    let project_remembered = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 7,
                "method": "memory/remember",
                "params": {
                    "text": "the repository uses Rust",
                    "scope": "project"
                }
            }),
        )
        .await
        .expect("project memory/remember response");
    assert!(
        project_remembered.get("result").is_some(),
        "project memory/remember failed: {project_remembered}"
    );
    let project_remembered: devo_protocol::native::rpc_memory::MemoryEntry =
        serde_json::from_value(project_remembered["result"].clone())?;
    assert_eq!(project_remembered.scope, MemoryScope::Project);
    assert_eq!(project_remembered.body, "the repository uses Rust");
    assert_eq!(project_remembered.scope_id.len(), 64);

    let project_listed = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 8,
                "method": "memory/list",
                "params": { "scope": "project" }
            }),
        )
        .await
        .expect("project memory/list response");
    let project_listed: Page<devo_protocol::native::rpc_memory::MemoryEntry> =
        serde_json::from_value(project_listed["result"].clone())?;
    assert_eq!(project_listed.data, vec![project_remembered.clone()]);
    assert_eq!(project_listed.next_cursor, None);

    let project_projection = data_root
        .path()
        .join("server")
        .join("memory")
        .join("projects")
        .join(&project_remembered.scope_id)
        .join("MEMORY.md");
    let projection = fs::read_to_string(project_projection)?;
    assert!(projection.contains("the repository uses Rust"));
    assert!(!projection.contains("I prefer dark mode"));
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-9, DD-12
/// Verifies: Native forget retires exact identities and returns ambiguous text matches without mutation.
#[tokio::test]
async fn native_memory_forget_supports_exact_and_ambiguous_requests() -> Result<()> {
    let data_root = TempDir::new()?;
    fs::create_dir_all(data_root.path().join(".devo"))?;
    fs::write(
        data_root.path().join(".devo").join("config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    let runtime = build_memory_test_runtime(data_root.path())?;
    let (notifications_tx, _notifications_rx) =
        devo_server::test_outbound_channel(/*capacity*/ 8);
    let connection_id = runtime
        .register_connection(ClientTransportKind::Stdio, notifications_tx)
        .await;
    let session_id = start_native_session(&runtime, connection_id, data_root.path()).await?;
    let _subscription = create_native_session_subscription(
        &runtime,
        connection_id,
        &session_id,
        /*request_id*/ 3,
    )
    .await?;

    let remembered = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 4,
                "method": "memory/remember",
                "params": { "text": "I prefer dark mode" }
            }),
        )
        .await
        .expect("memory/remember response");
    let remembered: devo_protocol::native::rpc_memory::MemoryEntry =
        serde_json::from_value(remembered["result"].clone())?;

    let forgotten = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 5,
                "method": "memory/forget",
                "params": { "entryId": remembered.entry_id }
            }),
        )
        .await
        .expect("memory/forget response");
    let forgotten: MemoryForgetResult = serde_json::from_value(forgotten["result"].clone())?;
    assert_eq!(forgotten.candidates, Vec::new());
    let forgotten_entry = forgotten.forgotten.expect("exact entry was retired");
    assert_eq!(forgotten_entry.entry_id, remembered.entry_id);
    assert_eq!(forgotten_entry.state, MemoryState::Retired);

    for text in ["I prefer tabs", "I prefer spaces"] {
        runtime
            .handle_incoming(
                connection_id,
                serde_json::json!({
                    "id": 6,
                    "method": "memory/remember",
                    "params": { "text": text }
                }),
            )
            .await
            .expect("memory/remember candidate response");
    }
    let ambiguous = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 7,
                "method": "memory/forget",
                "params": { "text": "I prefer" }
            }),
        )
        .await
        .expect("ambiguous memory/forget response");
    let ambiguous: MemoryForgetResult = serde_json::from_value(ambiguous["result"].clone())?;
    assert!(ambiguous.forgotten.is_none());
    assert_eq!(ambiguous.candidates.len(), 3);

    let active = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 8,
                "method": "memory/list",
                "params": { "scope": "user", "state": "active" }
            }),
        )
        .await
        .expect("active memory/list response");
    let active: Page<devo_protocol::native::rpc_memory::MemoryEntry> =
        serde_json::from_value(active["result"].clone())?;
    assert_eq!(active.data.len(), 2);
    Ok(())
}

/// Trace: L2-DES-MEM-001
/// Verifies: a committed entry appears only in a newly prepared memory snapshot.
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
        session_recall: devo_protocol::native::session::MemorySetting::Inherit,
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

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-3
/// Verifies: linked worktrees share Project memory while unrelated repositories remain isolated.
#[tokio::test]
async fn project_memory_shares_linked_worktrees_and_isolates_unrelated_repositories() {
    let data_root = TempDir::new().expect("memory data root");
    let repository_root = data_root.path().join("repository");
    let common_git_dir = repository_root.join(".git");
    let linked_root = data_root.path().join("linked-worktree");
    let linked_git_dir = common_git_dir.join("worktrees").join("linked");
    let unrelated_root = data_root.path().join("unrelated-repository");

    fs::create_dir_all(&common_git_dir).expect("create repository git directory");
    fs::create_dir_all(&linked_git_dir).expect("create linked git directory");
    fs::create_dir_all(&linked_root).expect("create linked worktree");
    let common_dir = Path::new("..").join("..");
    fs::write(
        linked_git_dir.join("commondir"),
        format!("{}\n", common_dir.display()),
    )
    .expect("write linked commondir");
    fs::write(
        linked_root.join(".git"),
        format!("gitdir: {}\n", linked_git_dir.display()),
    )
    .expect("write linked git file");
    fs::create_dir_all(unrelated_root.join(".git")).expect("create unrelated git directory");

    let runtime = MemoryRuntime::open(
        data_root.path().join("memory"),
        MemoryConfig {
            enabled: true,
            ..MemoryConfig::default()
        },
    )
    .expect("open enabled memory runtime");

    let main_entry = match runtime
        .execute_command(MemoryCommand::Remember(project_remember_request(
            "the repository uses Rust",
            "item-main",
            &repository_root,
        )))
        .await
        .expect("remember project memory from main checkout")
    {
        MemoryCommandResult::Remember(entry) => entry,
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_) => {
            panic!("unexpected project remember result")
        }
    };
    let linked_entry = match runtime
        .execute_command(MemoryCommand::Remember(project_remember_request(
            "the repository uses Rust",
            "item-linked",
            &linked_root,
        )))
        .await
        .expect("remember project memory from linked worktree")
    {
        MemoryCommandResult::Remember(entry) => entry,
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_) => {
            panic!("unexpected linked project remember result")
        }
    };
    assert_eq!(
        (
            linked_entry.scope,
            &linked_entry.scope_id,
            &linked_entry.entry_id
        ),
        (main_entry.scope, &main_entry.scope_id, &main_entry.entry_id)
    );

    let linked_list = match runtime
        .execute_command(MemoryCommand::List(ListMemoryRequest {
            scope: Some(MemoryScope::Project),
            workspace_root: linked_root,
            ..ListMemoryRequest::default()
        }))
        .await
        .expect("list linked project memory")
    {
        MemoryCommandResult::List(page) => page,
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::Remember(_) => {
            panic!("unexpected linked project list result")
        }
    };
    assert_eq!(linked_list.data, vec![linked_entry.clone()]);

    let unrelated_entry = match runtime
        .execute_command(MemoryCommand::Remember(project_remember_request(
            "the unrelated repository uses Python",
            "item-unrelated",
            &unrelated_root,
        )))
        .await
        .expect("remember unrelated project memory")
    {
        MemoryCommandResult::Remember(entry) => entry,
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_) => {
            panic!("unexpected unrelated project remember result")
        }
    };
    assert_ne!(unrelated_entry.scope_id, main_entry.scope_id);

    let main_list = match runtime
        .execute_command(MemoryCommand::List(ListMemoryRequest {
            scope: Some(MemoryScope::Project),
            workspace_root: repository_root,
            ..ListMemoryRequest::default()
        }))
        .await
        .expect("list main project memory")
    {
        MemoryCommandResult::List(page) => page,
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::Remember(_) => {
            panic!("unexpected main project list result")
        }
    };
    assert_eq!(main_list.data, vec![linked_entry]);
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-3
/// Verifies: Project memory follows the Native session selector across create and update.
#[tokio::test]
async fn native_project_memory_follows_native_subscription_selector() -> Result<()> {
    let data_root = TempDir::new()?;
    let project_a = data_root.path().join("project-a");
    let project_b = data_root.path().join("project-b");
    let project_c = data_root.path().join("project-c");
    fs::create_dir_all(&project_a)?;
    fs::create_dir_all(&project_b)?;
    fs::create_dir_all(&project_c)?;
    fs::create_dir_all(data_root.path().join(".devo"))?;
    fs::write(
        data_root.path().join(".devo").join("config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    let runtime = build_memory_test_runtime(data_root.path())?;
    let (connection_a_tx, _connection_a_rx) =
        devo_server::test_outbound_channel(/*capacity*/ 8);
    let connection_a = runtime
        .register_connection(ClientTransportKind::Stdio, connection_a_tx)
        .await;
    let (connection_b_tx, _connection_b_rx) =
        devo_server::test_outbound_channel(/*capacity*/ 8);
    let connection_b = runtime
        .register_connection(ClientTransportKind::Stdio, connection_b_tx)
        .await;
    let (connection_c_tx, _connection_c_rx) =
        devo_server::test_outbound_channel(/*capacity*/ 8);
    let connection_c = runtime
        .register_connection(ClientTransportKind::Stdio, connection_c_tx)
        .await;

    let _session_a = start_native_session(&runtime, connection_a, &project_a).await?;
    let session_b = start_native_session(&runtime, connection_b, &project_b).await?;
    let session_c = start_native_session(&runtime, connection_c, &project_c).await?;
    let subscription_a = create_native_session_subscription(
        &runtime,
        connection_a,
        &session_b,
        /*request_id*/ 3,
    )
    .await?;
    let _subscription_b = create_native_session_subscription(
        &runtime,
        connection_b,
        &session_b,
        /*request_id*/ 3,
    )
    .await?;
    let _subscription_c = create_native_session_subscription(
        &runtime,
        connection_c,
        &session_c,
        /*request_id*/ 3,
    )
    .await?;

    let project_b_entry = runtime
        .handle_incoming(
            connection_a,
            serde_json::json!({
                "id": 4,
                "method": "memory/remember",
                "params": {
                    "text": "project B uses Rust",
                    "scope": "project"
                }
            }),
        )
        .await
        .expect("Project B memory/remember response");
    let project_b_entry: devo_protocol::native::rpc_memory::MemoryEntry =
        serde_json::from_value(project_b_entry["result"].clone())?;

    let listed_from_b = runtime
        .handle_incoming(
            connection_b,
            serde_json::json!({
                "id": 4,
                "method": "memory/list",
                "params": { "scope": "project" }
            }),
        )
        .await
        .expect("Project B memory/list response");
    let listed_from_b: Page<devo_protocol::native::rpc_memory::MemoryEntry> =
        serde_json::from_value(listed_from_b["result"].clone())?;
    assert_eq!(listed_from_b.data, vec![project_b_entry.clone()]);

    let listed_from_native_selector = runtime
        .handle_incoming(
            connection_a,
            serde_json::json!({
                "id": 5,
                "method": "memory/list",
                "params": { "scope": "project" }
            }),
        )
        .await
        .expect("Project B list through Native selector response");
    let listed_from_native_selector: Page<devo_protocol::native::rpc_memory::MemoryEntry> =
        serde_json::from_value(listed_from_native_selector["result"].clone())?;
    assert_eq!(
        listed_from_native_selector.data,
        vec![project_b_entry.clone()]
    );

    let second_subscription_a = create_native_session_subscription(
        &runtime,
        connection_a,
        &session_c,
        /*request_id*/ 9,
    )
    .await?;
    let ambiguous_list = runtime
        .handle_incoming(
            connection_a,
            serde_json::json!({
                "id": 10,
                "method": "memory/list",
                "params": { "scope": "project" }
            }),
        )
        .await
        .expect("ambiguous Project memory/list response");
    let ambiguous_list: devo_protocol::ErrorResponse = serde_json::from_value(ambiguous_list)?;
    assert_eq!(ambiguous_list.error.code, ProtocolErrorCode::InvalidParams);
    assert_eq!(
        ambiguous_list.error.message,
        "memory/list Project scope has ambiguous Native Session selectors"
    );

    let ambiguous_remember = runtime
        .handle_incoming(
            connection_a,
            serde_json::json!({
                "id": 11,
                "method": "memory/remember",
                "params": {
                    "text": "ambiguous project memory",
                    "scope": "project"
                }
            }),
        )
        .await
        .expect("ambiguous Project memory/remember response");
    let ambiguous_remember: devo_protocol::ErrorResponse =
        serde_json::from_value(ambiguous_remember)?;
    assert_eq!(
        ambiguous_remember.error.message,
        "memory/remember Project scope has ambiguous Native Session selectors"
    );

    let unsubscribed = runtime
        .handle_incoming(
            connection_a,
            serde_json::json!({
                "id": 12,
                "method": "subscription/unsubscribe",
                "params": { "subscriptionId": second_subscription_a }
            }),
        )
        .await
        .expect("Native subscription/unsubscribe response");
    assert!(unsubscribed.get("result").is_some());

    let updated = runtime
        .handle_incoming(
            connection_a,
            serde_json::json!({
                "id": 6,
                "method": "subscription/update",
                "params": {
                    "subscriptionId": subscription_a,
                    "selectors": [{ "kind": "session", "sessionId": session_c }]
                }
            }),
        )
        .await
        .expect("Native subscription/update response");
    assert!(
        updated.get("result").is_some(),
        "subscription/update failed: {updated}"
    );

    let project_c_entry = runtime
        .handle_incoming(
            connection_a,
            serde_json::json!({
                "id": 7,
                "method": "memory/remember",
                "params": {
                    "text": "project C uses Python",
                    "scope": "project"
                }
            }),
        )
        .await
        .expect("Project C memory/remember response");
    let project_c_entry: devo_protocol::native::rpc_memory::MemoryEntry =
        serde_json::from_value(project_c_entry["result"].clone())?;
    assert_ne!(project_c_entry.scope_id, project_b_entry.scope_id);

    let listed_after_update = runtime
        .handle_incoming(
            connection_a,
            serde_json::json!({
                "id": 8,
                "method": "memory/list",
                "params": { "scope": "project" }
            }),
        )
        .await
        .expect("Project C list after selector update response");
    let listed_after_update: Page<devo_protocol::native::rpc_memory::MemoryEntry> =
        serde_json::from_value(listed_after_update["result"].clone())?;
    assert_eq!(listed_after_update.data, vec![project_c_entry]);
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-3
/// Verifies: Native Project memory resolves a durable session before resume.
#[tokio::test]
async fn native_project_memory_resolves_durable_session_after_restart() -> Result<()> {
    let data_root = TempDir::new()?;
    fs::create_dir_all(data_root.path().join(".devo"))?;
    fs::write(
        data_root.path().join(".devo").join("config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    let runtime = build_memory_test_runtime(data_root.path())?;
    let (connection_tx, _connection_rx) = devo_server::test_outbound_channel(/*capacity*/ 8);
    let connection_id = runtime
        .register_connection(ClientTransportKind::Stdio, connection_tx)
        .await;
    let session_id = start_native_session(&runtime, connection_id, data_root.path()).await?;
    drop(runtime);

    let runtime = build_memory_test_runtime(data_root.path())?;
    let (connection_tx, _connection_rx) = devo_server::test_outbound_channel(/*capacity*/ 8);
    let connection_id = runtime
        .register_connection(ClientTransportKind::Stdio, connection_tx)
        .await;
    initialize_native_connection(&runtime, connection_id).await?;
    create_native_session_subscription(&runtime, connection_id, &session_id, /*request_id*/ 2)
        .await?;

    let listed = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 3,
                "method": "memory/list",
                "params": { "scope": "project" }
            }),
        )
        .await
        .expect("Project memory/list after restart response");
    let listed: Page<devo_protocol::native::rpc_memory::MemoryEntry> =
        serde_json::from_value(listed["result"].clone())?;
    assert!(listed.data.is_empty());

    let remembered = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 4,
                "method": "memory/remember",
                "params": {
                    "text": "historical project memory",
                    "scope": "project"
                }
            }),
        )
        .await
        .expect("Project memory/remember after restart response");
    let remembered: devo_protocol::native::rpc_memory::MemoryEntry =
        serde_json::from_value(remembered["result"].clone())?;
    assert_eq!(remembered.body, "historical project memory");
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-3
/// Verifies: Native Project memory accepts multiple Session selectors for one Git project.
#[tokio::test]
async fn native_project_memory_accepts_same_project_session_selectors() -> Result<()> {
    let data_root = TempDir::new()?;
    let repository_root = data_root.path().join("repository");
    let common_git_dir = repository_root.join(".git");
    let linked_root = data_root.path().join("linked-worktree");
    let linked_git_dir = common_git_dir.join("worktrees").join("linked");
    fs::create_dir_all(&common_git_dir)?;
    fs::create_dir_all(&linked_git_dir)?;
    fs::create_dir_all(&linked_root)?;
    let common_dir = Path::new("..").join("..");
    fs::write(
        linked_git_dir.join("commondir"),
        format!("{}\n", common_dir.display()),
    )?;
    fs::write(
        linked_root.join(".git"),
        format!("gitdir: {}\n", linked_git_dir.display()),
    )?;
    fs::create_dir_all(data_root.path().join(".devo"))?;
    fs::write(
        data_root.path().join(".devo").join("config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    let runtime = build_memory_test_runtime(data_root.path())?;
    let (selector_tx, _selector_rx) = devo_server::test_outbound_channel(/*capacity*/ 8);
    let selector_connection = runtime
        .register_connection(ClientTransportKind::Stdio, selector_tx)
        .await;
    let (main_tx, _main_rx) = devo_server::test_outbound_channel(/*capacity*/ 8);
    let main_connection = runtime
        .register_connection(ClientTransportKind::Stdio, main_tx)
        .await;
    let (linked_tx, _linked_rx) = devo_server::test_outbound_channel(/*capacity*/ 8);
    let linked_connection = runtime
        .register_connection(ClientTransportKind::Stdio, linked_tx)
        .await;
    initialize_native_connection(&runtime, selector_connection).await?;
    let main_session = start_native_session(&runtime, main_connection, &repository_root).await?;
    let linked_session = start_native_session(&runtime, linked_connection, &linked_root).await?;
    create_native_session_subscription(
        &runtime,
        selector_connection,
        &main_session,
        /*request_id*/ 2,
    )
    .await?;
    create_native_session_subscription(
        &runtime,
        selector_connection,
        &linked_session,
        /*request_id*/ 3,
    )
    .await?;
    create_native_subscription(
        &runtime,
        selector_connection,
        serde_json::json!([{ "kind": "sessionsByCwd", "cwd": repository_root }]),
        /*request_id*/ 4,
    )
    .await?;

    let remembered = runtime
        .handle_incoming(
            selector_connection,
            serde_json::json!({
                "id": 5,
                "method": "memory/remember",
                "params": {
                    "text": "one Git project",
                    "scope": "project"
                }
            }),
        )
        .await
        .expect("same-project Project memory/remember response");
    let remembered: devo_protocol::native::rpc_memory::MemoryEntry =
        serde_json::from_value(remembered["result"].clone())?;
    assert_eq!(remembered.body, "one Git project");
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-3
/// Verifies: Project remember/list reject an active-turn versus selector project conflict.
#[tokio::test]
async fn native_project_memory_rejects_active_turn_selector_conflict() -> Result<()> {
    let data_root = TempDir::new()?;
    fs::create_dir_all(data_root.path().join(".devo"))?;
    fs::write(
        data_root.path().join(".devo").join("config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    fs::create_dir_all(data_root.path().join("other-project"))?;
    let release = Arc::new(tokio::sync::Notify::new());
    let provider: Arc<dyn ModelProviderSDK> = Arc::new(BlockingProvider {
        release: Arc::clone(&release),
    });
    let runtime = build_memory_test_runtime_with_provider(data_root.path(), provider)?;
    let (active_tx, mut active_notifications) =
        devo_server::test_outbound_channel(/*capacity*/ 16);
    let active_connection = runtime
        .register_connection(ClientTransportKind::Stdio, active_tx)
        .await;
    let (selector_tx, _selector_rx) = devo_server::test_outbound_channel(/*capacity*/ 8);
    let selector_connection = runtime
        .register_connection(ClientTransportKind::Stdio, selector_tx)
        .await;
    let active_session =
        start_native_session(&runtime, active_connection, data_root.path()).await?;
    let selector_session = start_native_session(
        &runtime,
        selector_connection,
        &data_root.path().join("other-project"),
    )
    .await?;
    create_native_session_subscription(
        &runtime,
        active_connection,
        &selector_session,
        /*request_id*/ 3,
    )
    .await?;

    let turn_start = runtime
        .handle_incoming(
            active_connection,
            serde_json::json!({
                "id": 4,
                "method": "turn/start",
                "params": {
                    "sessionId": active_session,
                    "input": [{ "type": "text", "text": "active project" }],
                    "idempotencyKey": "active-project-conflict"
                }
            }),
        )
        .await
        .expect("active turn/start response");
    assert!(
        turn_start.get("result").is_some(),
        "turn/start failed: {turn_start}"
    );
    let source_user_item_id = loop {
        let notification = tokio::time::timeout(
            Duration::from_secs(/*seconds*/ 2),
            active_notifications.recv(),
        )
        .await?
        .context("active turn notification channel closed")?;
        if notification["method"] == "item/started"
            && notification["params"]["item"]["sessionId"] == active_session.to_string()
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
                    "text": "conflicting project memory",
                    "scope": "project",
                    "sourceUserItemId": source_user_item_id
                }
            }),
        )
        .await
        .expect("conflicting Project memory/remember response");
    let remembered: devo_protocol::ErrorResponse = serde_json::from_value(remembered)?;
    assert_eq!(remembered.error.code, ProtocolErrorCode::InvalidParams);
    assert_eq!(
        remembered.error.message,
        "memory/remember Project scope has ambiguous Native Session selectors"
    );

    let listed = runtime
        .handle_incoming(
            active_connection,
            serde_json::json!({
                "id": 6,
                "method": "memory/list",
                "params": { "scope": "project" }
            }),
        )
        .await
        .expect("conflicting Project memory/list response");
    let listed: devo_protocol::ErrorResponse = serde_json::from_value(listed)?;
    assert_eq!(listed.error.code, ProtocolErrorCode::InvalidParams);
    assert_eq!(
        listed.error.message,
        "memory/list Project scope has ambiguous Native Session selectors"
    );
    release.notify_waiters();
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-3
/// Verifies: Project remember/list reject multiple active sessions from different projects.
#[tokio::test]
async fn native_project_memory_rejects_multiple_active_project_scopes() -> Result<()> {
    let data_root = TempDir::new()?;
    let other_root = data_root.path().join("other-project");
    fs::create_dir_all(data_root.path().join(".devo"))?;
    fs::create_dir_all(&other_root)?;
    fs::write(
        data_root.path().join(".devo").join("config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    let release = Arc::new(tokio::sync::Notify::new());
    let provider: Arc<dyn ModelProviderSDK> = Arc::new(BlockingProvider {
        release: Arc::clone(&release),
    });
    let runtime = build_memory_test_runtime_with_provider(data_root.path(), provider)?;
    let (active_tx, mut active_notifications) =
        devo_server::test_outbound_channel(/*capacity*/ 16);
    let connection_id = runtime
        .register_connection(ClientTransportKind::Stdio, active_tx)
        .await;
    initialize_native_connection(&runtime, connection_id).await?;
    let session_a = start_native_session_after_initialize(
        &runtime,
        connection_id,
        data_root.path(),
        /*request_id*/ 2,
    )
    .await?;
    let session_b = start_native_session_after_initialize(
        &runtime,
        connection_id,
        &other_root,
        /*request_id*/ 3,
    )
    .await?;

    for (request_id, session_id, input) in [
        (4, &session_a, "active project A"),
        (5, &session_b, "active project B"),
    ] {
        let response = runtime
            .handle_incoming(
                connection_id,
                serde_json::json!({
                    "id": request_id,
                    "method": "turn/start",
                    "params": {
                        "sessionId": session_id,
                        "input": [{ "type": "text", "text": input }],
                        "idempotencyKey": format!("multi-active-{request_id}")
                    }
                }),
            )
            .await
            .expect("active turn/start response");
        assert!(
            response.get("result").is_some(),
            "turn/start failed: {response}"
        );
    }

    let mut source_user_item_id = None;
    let mut started_sessions = 0;
    while started_sessions < 2 {
        let notification = tokio::time::timeout(
            Duration::from_secs(/*seconds*/ 2),
            active_notifications.recv(),
        )
        .await?
        .context("active turn notification channel closed")?;
        if notification["method"] != "item/started" {
            continue;
        }
        let session_id = notification["params"]["item"]["sessionId"]
            .as_str()
            .context("session id in item/started")?;
        if session_id != session_a && session_id != session_b {
            continue;
        }
        started_sessions += 1;
        if session_id == session_a {
            source_user_item_id = Some(
                notification["params"]["item"]["id"]
                    .as_str()
                    .context("user item id in item/started")?
                    .to_string(),
            );
        }
    }
    let source_user_item_id = source_user_item_id.context("session A item/started")?;

    let listed = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 6,
                "method": "memory/list",
                "params": { "scope": "project" }
            }),
        )
        .await
        .expect("ambiguous active Project memory/list response");
    let listed: devo_protocol::ErrorResponse = serde_json::from_value(listed)?;
    assert_eq!(listed.error.code, ProtocolErrorCode::InvalidParams);
    assert_eq!(
        listed.error.message,
        "memory/list Project scope has ambiguous Native Session selectors"
    );

    let remembered = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 7,
                "method": "memory/remember",
                "params": {
                    "text": "ambiguous active project memory",
                    "scope": "project",
                    "sourceUserItemId": source_user_item_id
                }
            }),
        )
        .await
        .expect("ambiguous active Project memory/remember response");
    let remembered: devo_protocol::ErrorResponse = serde_json::from_value(remembered)?;
    assert_eq!(remembered.error.code, ProtocolErrorCode::InvalidParams);
    assert_eq!(
        remembered.error.message,
        "memory/remember Project scope has ambiguous Native Session selectors"
    );
    release.notify_waiters();
    Ok(())
}
