use std::sync::Arc;

use anyhow::{Context, Result};
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::{
    MemoryEntry, MemoryForgetResult, MemoryScope, MemoryState,
};
use devo_protocol::{ErrorResponse, ProtocolError, ProtocolErrorCode};
use pretty_assertions::assert_eq;

#[path = "support/memory_forget_runtime.rs"]
#[allow(dead_code)]
mod memory_forget_runtime_support;
#[path = "support/memory_forget.rs"]
#[allow(dead_code)]
mod memory_forget_support;
#[path = "support/subagent_lifecycle.rs"]
#[allow(dead_code)]
mod support;

use memory_forget_runtime_support::{configured_data_root, remember, start_subscribed_session};
use memory_forget_support::BlockingFirstMemoryCommandExecutor;
use support::{ScriptedProvider, build_runtime_with_overrides, start_turn_with_approval_policy};

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
/// Verifies: Project identity validation finishes before Native forget attempts to acquire its lease.
#[tokio::test]
async fn project_forget_resolves_fallible_source_before_lease() -> Result<()> {
    const SUBSCRIPTION_REQUEST_ID: u64 = 20;
    const REMEMBER_REQUEST_ID: u64 = 21;
    const USER_FORGET_REQUEST_ID: u64 = 22;
    const PROJECT_FORGET_REQUEST_ID: u64 = 23;

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
        Some(&workspace_root),
        Some(Arc::clone(&executor) as _),
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
        /*request_id*/ REMEMBER_REQUEST_ID,
        "I prefer tabs",
    )
    .await?;
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
                "params": {
                    "entryId": "mem_missing",
                    "scope": MemoryScope::Project,
                }
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
