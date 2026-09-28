use super::*;
use devo_protocol::native::rpc_memory::{
    MemoryEntry, MemoryForgetResult, MemoryProvenance, MemoryState,
};
use pretty_assertions::assert_eq;

async fn remember_and_forget_user(
    runtime: &Arc<ServerRuntime>,
    connection_id: u64,
    session_id: &str,
    text: &str,
) -> Result<MemoryEntry> {
    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 40, "method": "memory/remember",
                "params": { "text": text, "scope": "user" }
            }),
        )
        .await
        .context("User remember response")?;
    let entry: MemoryEntry = serde_json::from_value(response["result"].clone())
        .with_context(|| format!("User remember failed: {response}"))?;
    let response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 41, "method": "memory/forget",
                // The stored entry determines User scope even when scope is omitted.
                "params": { "entryId": entry.entry_id }
            }),
        )
        .await
        .context("User forget response")?;
    let forgotten: MemoryForgetResult = serde_json::from_value(response["result"].clone())
        .with_context(|| format!("User forget failed: {response}"))?;
    let expected_provenance = vec![MemoryProvenance {
        source_session_id: Some(session_id.to_string()),
        source_turn_id: None,
        source_user_item_id: None,
    }];
    assert_eq!(entry.provenance, expected_provenance);
    assert_eq!(
        (
            forgotten.forgotten.map(|entry| (
                entry.entry_id,
                entry.scope,
                entry.state,
                entry.provenance
            )),
            forgotten.candidates
        ),
        (
            Some((
                entry.entry_id.clone(),
                MemoryScope::User,
                MemoryState::Retired,
                expected_provenance
            )),
            Vec::new()
        )
    );
    Ok(entry)
}

/// Trace: L2-DES-APP-008 Rev 5 DD-1, L2-DES-MEM-001 Rev 4 DD-6/DD-12
/// Verifies: User provenance and exact-ID deletion follow Native selector create/update and legacy fallback.
#[tokio::test]
async fn user_memory_commands_follow_native_selector_and_legacy_fallback() -> Result<()> {
    let data_root = TempDir::new()?;
    fs::create_dir_all(data_root.path().join(".devo"))?;
    fs::write(
        data_root.path().join(".devo/config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    let runtime = build_memory_test_runtime(data_root.path())?;
    let mut connections = Vec::new();
    let mut sessions = Vec::new();
    for name in ["project-a", "project-b", "project-c"] {
        let root = data_root.path().join(name);
        fs::create_dir_all(&root)?;
        let (tx, _rx) = devo_server::test_outbound_channel(/*capacity*/ 8);
        let connection = runtime
            .register_connection(ClientTransportKind::Stdio, tx)
            .await;
        sessions.push(start_native_session(&runtime, connection, &root).await?);
        connections.push(connection);
    }
    let connection = connections[0];
    let subscription = create_native_session_subscription(
        &runtime,
        connection,
        &sessions[1],
        /*request_id*/ 3,
    )
    .await?;
    remember_and_forget_user(
        &runtime,
        connection,
        &sessions[1],
        "I prefer concise reviews.",
    )
    .await?;
    let updated = runtime
        .handle_incoming(
            connection,
            serde_json::json!({
                "id": 4, "method": "subscription/update",
                "params": { "subscriptionId": subscription,
                    "selectors": [{ "kind": "session", "sessionId": sessions[2] }] }
            }),
        )
        .await
        .context("Native selector update")?;
    anyhow::ensure!(
        updated.get("result").is_some(),
        "selector update failed: {updated}"
    );
    let user_entry = remember_and_forget_user(
        &runtime,
        connection,
        &sessions[2],
        "I prefer detailed release notes.",
    )
    .await?;

    let second_subscription = create_native_session_subscription(
        &runtime,
        connection,
        &sessions[1],
        /*request_id*/ 5,
    )
    .await?;
    for (id, method, params) in [
        (
            6,
            "memory/remember",
            serde_json::json!({ "text": "I prefer dark mode.", "scope": "user" }),
        ),
        (
            7,
            "memory/forget",
            serde_json::json!({ "entryId": user_entry.entry_id }),
        ),
    ] {
        let response = runtime
            .handle_incoming(
                connection,
                serde_json::json!({
                    "id": id, "method": method, "params": params
                }),
            )
            .await
            .context("ambiguous User command")?;
        assert_eq!(
            response,
            serde_json::json!({ "id": id, "error": {
                "code": "InvalidParams", "data": {},
                "message": format!("{method} User scope has ambiguous Native Session selectors")
            }})
        );
    }

    // Project exact-ID deletion remains resolvable with multiple selectors.
    let response = runtime
        .handle_incoming(
            connections[1],
            serde_json::json!({
                "id": 8, "method": "memory/remember",
                "params": { "text": "Project B uses Rust.", "scope": "project" }
            }),
        )
        .await
        .context("Project remember")?;
    let project_entry: MemoryEntry = serde_json::from_value(response["result"].clone())?;
    let response = runtime
        .handle_incoming(
            connection,
            serde_json::json!({
                "id": 9, "method": "memory/forget", "params": { "entryId": project_entry.entry_id }
            }),
        )
        .await
        .context("Project exact forget")?;
    let forgotten: MemoryForgetResult = serde_json::from_value(response["result"].clone())
        .with_context(|| format!("Project forget failed: {response}"))?;
    assert_eq!(
        forgotten
            .forgotten
            .map(|entry| (entry.entry_id, entry.scope, entry.state)),
        Some((
            project_entry.entry_id,
            MemoryScope::Project,
            MemoryState::Retired
        ))
    );
    for (id, subscription_id) in [(10, subscription), (11, second_subscription)] {
        let response = runtime
            .handle_incoming(
                connection,
                serde_json::json!({
                    "id": id, "method": "subscription/unsubscribe",
                    "params": { "subscriptionId": subscription_id }
                }),
            )
            .await
            .context("unsubscribe Native selector")?;
        anyhow::ensure!(
            response.get("result").is_some(),
            "unsubscribe failed: {response}"
        );
    }
    remember_and_forget_user(
        &runtime,
        connection,
        &sessions[0],
        "I prefer short status updates.",
    )
    .await?;
    Ok(())
}
