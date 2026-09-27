use std::sync::Arc;

use anyhow::{Context, Result};
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::{
    MemoryEntry, MemoryForgetResult, MemoryScope, MemoryState,
};
use devo_protocol::{ErrorResponse, ProtocolError, ProtocolErrorCode};
use pretty_assertions::assert_eq;
use rusqlite::Connection;

use crate::memory_forget_runtime_support::{
    configured_data_root, remember, start_subscribed_session,
};
use crate::memory_forget_support::{
    BlockingFirstMemoryCommandExecutor, build_runtime_with_overrides,
};
use crate::support::{ScriptedProvider, start_parent_session, start_turn_with_approval_policy};

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-9, DD-12
/// Verifies: Native forget retires exact identities and returns ambiguous text matches without mutation.
#[tokio::test]
async fn native_forget_supports_exact_and_ambiguous_requests() -> Result<()> {
    const SUBSCRIPTION_REQUEST_ID: u64 = 10;
    const FIRST_REMEMBER_REQUEST_ID: u64 = 11;
    const EXACT_FORGET_REQUEST_ID: u64 = 12;
    const SECOND_REMEMBER_REQUEST_ID: u64 = 13;
    const THIRD_REMEMBER_REQUEST_ID: u64 = 14;
    const AMBIGUOUS_FORGET_REQUEST_ID: u64 = 15;
    const LIST_REQUEST_ID: u64 = 16;

    let data_root = configured_data_root()?;
    let provider = Arc::new(ScriptedProvider::new([]));
    let runtime = build_runtime_with_overrides(
        data_root.path(),
        Arc::clone(&provider) as _,
        /*workspace_root*/ Some(data_root.path()),
        /*memory_command_executor*/ None,
    )?;
    let (connection_id, _notifications, _session_id) = start_subscribed_session(
        &runtime,
        data_root.path(),
        /*request_id*/ SUBSCRIPTION_REQUEST_ID,
    )
    .await?;
    let remembered = remember(
        &runtime,
        connection_id,
        /*request_id*/ FIRST_REMEMBER_REQUEST_ID,
        "I prefer dark mode",
    )
    .await?;

    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": EXACT_FORGET_REQUEST_ID,
                "method": "memory/forget",
                "params": { "entryId": remembered.entry_id }
            }),
        )
        .await
        .context("exact memory/forget response")?;
    let response: devo_server::SuccessResponse<MemoryForgetResult> =
        serde_json::from_value(response).context("decode exact memory/forget response")?;
    let forgotten = response
        .result
        .forgotten
        .clone()
        .context("forgotten entry")?;
    let expected_forgotten = MemoryEntry {
        state: MemoryState::Retired,
        updated_at: forgotten.updated_at,
        ..remembered
    };
    assert_eq!(
        response,
        devo_server::SuccessResponse {
            id: serde_json::json!(EXACT_FORGET_REQUEST_ID),
            result: MemoryForgetResult {
                forgotten: Some(expected_forgotten.clone()),
                candidates: Vec::new(),
            },
        }
    );

    let mut expected_candidates = vec![expected_forgotten];
    expected_candidates.push(
        remember(
            &runtime,
            connection_id,
            /*request_id*/ SECOND_REMEMBER_REQUEST_ID,
            "I prefer tabs",
        )
        .await?,
    );
    expected_candidates.push(
        remember(
            &runtime,
            connection_id,
            /*request_id*/ THIRD_REMEMBER_REQUEST_ID,
            "I prefer spaces",
        )
        .await?,
    );
    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": AMBIGUOUS_FORGET_REQUEST_ID,
                "method": "memory/forget",
                "params": { "text": "I prefer" }
            }),
        )
        .await
        .context("ambiguous memory/forget response")?;
    let response: devo_server::SuccessResponse<MemoryForgetResult> =
        serde_json::from_value(response).context("decode ambiguous memory/forget response")?;
    let mut actual_candidates = response.result.candidates;
    actual_candidates.sort_by_key(|entry| entry.entry_id.to_string());
    expected_candidates.sort_by_key(|entry| entry.entry_id.to_string());
    assert_eq!(
        MemoryForgetResult {
            forgotten: response.result.forgotten,
            candidates: actual_candidates,
        },
        MemoryForgetResult {
            forgotten: None,
            candidates: expected_candidates.clone(),
        }
    );

    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": LIST_REQUEST_ID,
                "method": "memory/list",
                "params": { "scope": MemoryScope::User, "state": MemoryState::Active }
            }),
        )
        .await
        .context("active memory/list response")?;
    let mut active: devo_server::SuccessResponse<Page<MemoryEntry>> =
        serde_json::from_value(response).context("decode active memory/list response")?;
    active
        .result
        .data
        .sort_by_key(|entry| entry.entry_id.to_string());
    assert_eq!(
        active,
        devo_server::SuccessResponse {
            id: serde_json::json!(LIST_REQUEST_ID),
            result: Page {
                data: expected_candidates
                    .into_iter()
                    .filter(|entry| entry.state == MemoryState::Active)
                    .collect(),
                next_cursor: None,
            },
        }
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev4 DD-6, DD-12
/// Verifies: an explicit Native forget command does not require agent provenance during an active turn.
#[tokio::test]
async fn direct_native_forget_during_active_turn_needs_no_source_item() -> Result<()> {
    const SUBSCRIPTION_REQUEST_ID: u64 = 1;
    const REMEMBER_REQUEST_ID: u64 = 2;
    const FORGET_REQUEST_ID: u64 = 3;

    let data_root = configured_data_root()?;
    let provider = Arc::new(ScriptedProvider::pending());
    let runtime = build_runtime_with_overrides(
        data_root.path(),
        Arc::clone(&provider) as _,
        /*workspace_root*/ Some(data_root.path()),
        /*memory_command_executor*/ None,
    )?;
    let (connection_id, _notifications, session_id) = start_subscribed_session(
        &runtime,
        data_root.path(),
        /*request_id*/ SUBSCRIPTION_REQUEST_ID,
    )
    .await?;
    let remembered = remember(
        &runtime,
        connection_id,
        /*request_id*/ REMEMBER_REQUEST_ID,
        "I prefer tabs",
    )
    .await?;

    start_turn_with_approval_policy(
        &runtime,
        connection_id,
        session_id,
        "Keep working",
        Some("never"),
    )
    .await?;
    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": FORGET_REQUEST_ID,
                "method": "memory/forget",
                "params": { "entryId": remembered.entry_id }
            }),
        )
        .await
        .context("direct Native memory/forget response")?;
    let response: devo_server::SuccessResponse<MemoryForgetResult> =
        serde_json::from_value(response).context("decode memory/forget response")?;
    let result = response.result;
    let forgotten = result.forgotten.clone().context("forgotten entry")?;
    assert_eq!(
        devo_server::SuccessResponse {
            id: serde_json::json!(FORGET_REQUEST_ID),
            result,
        },
        devo_server::SuccessResponse {
            id: serde_json::json!(FORGET_REQUEST_ID),
            result: MemoryForgetResult {
                forgotten: Some(MemoryEntry {
                    state: MemoryState::Retired,
                    updated_at: forgotten.updated_at,
                    ..remembered
                }),
                candidates: Vec::new(),
            },
        }
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev4 DD-12
/// Verifies: an exact Project ID with omitted scope validates identity before Native forget acquires its lease.
#[tokio::test]
async fn project_forget_resolves_fallible_source_before_lease() -> Result<()> {
    const SUBSCRIPTION_REQUEST_ID: u64 = 20;
    const USER_REMEMBER_REQUEST_ID: u64 = 21;
    const PROJECT_REMEMBER_REQUEST_ID: u64 = 22;
    const USER_FORGET_REQUEST_ID: u64 = 23;
    const PROJECT_FORGET_REQUEST_ID: u64 = 24;

    let data_root = configured_data_root()?;
    let workspace_root = data_root.path().join("removed-project");
    std::fs::create_dir_all(workspace_root.join(".devo"))?;
    std::fs::write(
        workspace_root.join(".devo").join("config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    let provider = Arc::new(ScriptedProvider::new([]));
    let executor = Arc::new(BlockingFirstMemoryCommandExecutor::new());
    let runtime = build_runtime_with_overrides(
        data_root.path(),
        Arc::clone(&provider) as _,
        /*workspace_root*/ Some(&workspace_root),
        /*memory_command_executor*/ Some(Arc::clone(&executor) as _),
    )?;
    let (connection_id, _notifications, _session_id) = start_subscribed_session(
        &runtime,
        &workspace_root,
        /*request_id*/ SUBSCRIPTION_REQUEST_ID,
    )
    .await?;
    let remembered = remember(
        &runtime,
        connection_id,
        /*request_id*/ USER_REMEMBER_REQUEST_ID,
        "I prefer tabs",
    )
    .await?;
    let project_response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": PROJECT_REMEMBER_REQUEST_ID,
                "method": "memory/remember",
                "params": {
                    "text": "The project uses tabs",
                    "scope": MemoryScope::Project,
                }
            }),
        )
        .await
        .context("Project memory/remember response")?;
    let project_entry: MemoryEntry = serde_json::from_value(project_response["result"].clone())?;
    let first_runtime = Arc::clone(&runtime);
    let first_forget = tokio::spawn(async move {
        first_runtime
            .handle_incoming(
                connection_id,
                serde_json::json!({
                    "id": USER_FORGET_REQUEST_ID,
                    "method": "memory/forget",
                    "params": { "entryId": remembered.entry_id }
                }),
            )
            .await
    });
    executor.wait_until_started().await?;
    std::fs::remove_dir_all(&workspace_root)?;

    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": PROJECT_FORGET_REQUEST_ID,
                "method": "memory/forget",
                "params": { "entryId": project_entry.entry_id }
            }),
        )
        .await
        .context("Project memory/forget response")?;

    assert_eq!(
        serde_json::from_value::<ErrorResponse>(response)?,
        ErrorResponse {
            id: serde_json::json!(PROJECT_FORGET_REQUEST_ID),
            error: ProtocolError {
                code: ProtocolErrorCode::InvalidParams,
                message: "memory/forget Project scope identity is unavailable".to_string(),
                data: serde_json::json!({}),
            },
        }
    );
    executor.release();
    first_forget.await?.context("User memory/forget response")?;
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev4 DD-12
/// Verifies: Native exact-ID forget selects the Project session matching the stored target scope.
#[tokio::test]
async fn exact_project_forget_ignores_unrelated_project_sessions() -> Result<()> {
    const PROJECT_A_SUBSCRIPTION_REQUEST_ID: u64 = 60;
    const PROJECT_REMEMBER_REQUEST_ID: u64 = 61;
    const PROJECT_B_SUBSCRIPTION_REQUEST_ID: u64 = 62;
    const PROJECT_FORGET_REQUEST_ID: u64 = 63;

    let data_root = configured_data_root()?;
    let project_a = data_root.path().join("project-a");
    let project_b = data_root.path().join("project-b");
    std::fs::create_dir_all(&project_a)?;
    std::fs::create_dir_all(&project_b)?;
    let provider = Arc::new(ScriptedProvider::new([]));
    let runtime = build_runtime_with_overrides(
        data_root.path(),
        Arc::clone(&provider) as _,
        /*workspace_root*/ Some(data_root.path()),
        /*memory_command_executor*/ None,
    )?;
    let (connection_id, _notifications, _project_a_session) = start_subscribed_session(
        &runtime,
        &project_a,
        /*request_id*/ PROJECT_A_SUBSCRIPTION_REQUEST_ID,
    )
    .await?;
    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": PROJECT_REMEMBER_REQUEST_ID,
                "method": "memory/remember",
                "params": {
                    "text": "Project A uses tabs",
                    "scope": MemoryScope::Project,
                }
            }),
        )
        .await
        .context("Project A memory/remember response")?;
    let project_entry: MemoryEntry = serde_json::from_value(response["result"].clone())
        .with_context(|| format!("decode Project A memory entry: {response}"))?;
    let project_b_session = start_parent_session(&runtime, connection_id, &project_b).await?;
    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": PROJECT_B_SUBSCRIPTION_REQUEST_ID,
                "method": "subscription/create",
                "params": {
                    "selectors": [{ "kind": "session", "sessionId": project_b_session }],
                    "includeSnapshot": false
                }
            }),
        )
        .await
        .context("Project B subscription/create response")?;
    anyhow::ensure!(
        response.get("result").is_some(),
        "Project B subscription/create failed: {response}"
    );

    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": PROJECT_FORGET_REQUEST_ID,
                "method": "memory/forget",
                "params": { "entryId": project_entry.entry_id }
            }),
        )
        .await
        .context("Project A memory/forget response")?;
    let response: devo_server::SuccessResponse<MemoryForgetResult> =
        serde_json::from_value(response.clone())
            .with_context(|| format!("decode Project A memory/forget response: {response}"))?;
    let forgotten = response
        .result
        .forgotten
        .clone()
        .context("forgotten Project A entry")?;
    assert_eq!(
        response,
        devo_server::SuccessResponse {
            id: serde_json::json!(PROJECT_FORGET_REQUEST_ID),
            result: MemoryForgetResult {
                forgotten: Some(MemoryEntry {
                    state: MemoryState::Retired,
                    updated_at: forgotten.updated_at,
                    ..project_entry
                }),
                candidates: Vec::new(),
            },
        }
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev4 DD-12
/// Verifies: invalid Native text is rejected before deletion-lease admission.
#[tokio::test]
async fn invalid_text_is_rejected_before_active_lease() -> Result<()> {
    const SUBSCRIPTION_REQUEST_ID: u64 = 70;
    const REMEMBER_REQUEST_ID: u64 = 71;
    const EXACT_FORGET_REQUEST_ID: u64 = 72;
    const TEXT_FORGET_REQUEST_ID: u64 = 73;

    let data_root = configured_data_root()?;
    let provider = Arc::new(ScriptedProvider::new([]));
    let executor = Arc::new(BlockingFirstMemoryCommandExecutor::new());
    let runtime = build_runtime_with_overrides(
        data_root.path(),
        Arc::clone(&provider) as _,
        /*workspace_root*/ Some(data_root.path()),
        /*memory_command_executor*/ Some(Arc::clone(&executor) as _),
    )?;
    let (connection_id, _notifications, _session_id) = start_subscribed_session(
        &runtime,
        data_root.path(),
        /*request_id*/ SUBSCRIPTION_REQUEST_ID,
    )
    .await?;
    let remembered = remember(
        &runtime,
        connection_id,
        /*request_id*/ REMEMBER_REQUEST_ID,
        "I prefer tabs",
    )
    .await?;

    let held_runtime = Arc::clone(&runtime);
    let held_forget = tokio::spawn(async move {
        held_runtime
            .handle_incoming(
                connection_id,
                serde_json::json!({
                    "id": EXACT_FORGET_REQUEST_ID,
                    "method": "memory/forget",
                    "params": { "entryId": remembered.entry_id }
                }),
            )
            .await
    });
    executor.wait_until_started().await?;

    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": TEXT_FORGET_REQUEST_ID,
                "method": "memory/forget",
                "params": { "text": "   " }
            }),
        )
        .await
        .context("invalid text memory/forget response")?;
    executor.release();
    held_forget.await?.context("exact memory/forget response")?;

    assert_eq!(
        serde_json::from_value::<ErrorResponse>(response)?,
        ErrorResponse {
            id: serde_json::json!(TEXT_FORGET_REQUEST_ID),
            error: ProtocolError {
                code: ProtocolErrorCode::InvalidParams,
                message: "memory text must not be empty".to_string(),
                data: serde_json::json!({}),
            },
        }
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev4 DD-6, DD-12
/// Verifies: Project identity resolution does not depend on unrelated User recall data.
#[tokio::test]
async fn project_forget_does_not_read_user_recall_entries() -> Result<()> {
    const SUBSCRIPTION_REQUEST_ID: u64 = 80;
    const USER_REMEMBER_REQUEST_ID: u64 = 81;
    const PROJECT_REMEMBER_REQUEST_ID: u64 = 82;
    const PROJECT_FORGET_REQUEST_ID: u64 = 83;

    let data_root = configured_data_root()?;
    let workspace_root = data_root.path().join("project");
    std::fs::create_dir_all(workspace_root.join(".devo"))?;
    std::fs::write(
        workspace_root.join(".devo").join("config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    let provider = Arc::new(ScriptedProvider::new([]));
    let runtime = build_runtime_with_overrides(
        data_root.path(),
        Arc::clone(&provider) as _,
        /*workspace_root*/ Some(&workspace_root),
        /*memory_command_executor*/ None,
    )?;
    let (connection_id, _notifications, _session_id) = start_subscribed_session(
        &runtime,
        &workspace_root,
        /*request_id*/ SUBSCRIPTION_REQUEST_ID,
    )
    .await?;
    let user_entry = remember(
        &runtime,
        connection_id,
        /*request_id*/ USER_REMEMBER_REQUEST_ID,
        "I prefer tabs",
    )
    .await?;
    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": PROJECT_REMEMBER_REQUEST_ID,
                "method": "memory/remember",
                "params": {
                    "text": "The project uses tabs",
                    "scope": MemoryScope::Project,
                }
            }),
        )
        .await
        .context("Project memory/remember response")?;
    let project_entry: MemoryEntry = serde_json::from_value(response["result"].clone())?;
    let connection = Connection::open(data_root.path().join("memory").join("memory.sqlite3"))?;
    connection.execute(
        "UPDATE memory_entries SET origin = 'invalid' WHERE entry_id = ?1",
        [user_entry.entry_id.as_str()],
    )?;

    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": PROJECT_FORGET_REQUEST_ID,
                "method": "memory/forget",
                "params": { "entryId": project_entry.entry_id }
            }),
        )
        .await
        .context("Project memory/forget response")?;
    let response: devo_server::SuccessResponse<MemoryForgetResult> =
        serde_json::from_value(response.clone())
            .with_context(|| format!("decode Project memory/forget response: {response}"))?;
    let forgotten = response
        .result
        .forgotten
        .clone()
        .context("forgotten Project entry")?;
    assert_eq!(
        response,
        devo_server::SuccessResponse {
            id: serde_json::json!(PROJECT_FORGET_REQUEST_ID),
            result: MemoryForgetResult {
                forgotten: Some(MemoryEntry {
                    state: MemoryState::Retired,
                    updated_at: forgotten.updated_at,
                    ..project_entry
                }),
                candidates: Vec::new(),
            },
        }
    );
    Ok(())
}
