use super::*;

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-6, DD-12
/// Verifies: a user-created fork remains a root-agent session and can forget an exact stable ID.
#[tokio::test]
async fn fork_root_agent_can_forget_exact_stable_id() -> Result<()> {
    let provider = Arc::new(MemoryAgentProvider::new([
        ProviderAction::ForgetTarget,
        ProviderAction::Complete("fork memory forgotten"),
    ]));
    let mut harness = MemoryAgentHarness::new(Arc::clone(&provider)).await?;
    let entry = harness.remember("I prefer tabs", MemoryScope::User).await?;
    provider.set_target(entry.entry_id.clone());

    let fork_response = harness
        .runtime
        .handle_incoming(
            harness.connection_id,
            serde_json::json!({
                "id": 7,
                "method": "session/fork",
                "params": { "sessionId": harness.session_id }
            }),
        )
        .await
        .context("session/fork response")?;
    let fork = serde_json::from_value::<
        devo_server::SuccessResponse<devo_protocol::native::rpc_session::SessionForkResult>,
    >(fork_response)?
    .result;
    let fork_session_id = SessionId::try_from(fork.session.id.as_str())?;
    let subscription_response = harness
        .runtime
        .handle_incoming(
            harness.connection_id,
            serde_json::json!({
                "id": 8,
                "method": "subscription/create",
                "params": {
                    "selectors": [{ "kind": "session", "sessionId": fork_session_id }],
                    "includeSnapshot": false
                }
            }),
        )
        .await
        .context("fork subscription/create response")?;
    anyhow::ensure!(
        subscription_response.get("result").is_some(),
        "fork subscription/create failed: {subscription_response}"
    );
    harness.session_id = fork_session_id;

    harness
        .run_turn(&format!("Forget memory entry {}", entry.entry_id))
        .await?;

    let requests = provider.requests();
    let raw_result =
        tool_result(&requests[1], "memory-forget").context("fork memory forget result")?;
    let result: MemoryForgetResult = serde_json::from_str(raw_result)
        .with_context(|| format!("decode fork memory forget result: {raw_result}"))?;
    let forgotten = result.forgotten.clone().context("forgotten entry")?;
    pretty_assertions::assert_eq!(
        result,
        MemoryForgetResult {
            forgotten: Some(MemoryEntry {
                state: MemoryState::Retired,
                updated_at: forgotten.updated_at,
                ..entry
            }),
            candidates: Vec::new(),
        }
    );
    Ok(())
}
