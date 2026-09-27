use super::*;
use devo_protocol::native::session::MemorySetting;
use pretty_assertions::assert_eq;

/// Trace: L2-DES-APP-008 Rev 5 DD-5, L2-DES-CONV-002 Rev 2 DD-2, L2-DES-MEM-001 Rev 3 Session Controls
/// Verifies: partial Native metadata updates preserve independent memory settings in an ephemeral session.
#[tokio::test]
async fn ephemeral_memory_settings_survive_metadata_updates() -> Result<()> {
    let data_root = TempDir::new()?;
    let runtime = build_runtime(data_root.path());
    let connection_id = initialized_connection(&runtime).await;

    let start_response = runtime
        .start_session_with_registry(
            connection_id,
            serde_json::json!(100),
            SessionStartParams {
                cwd: data_root.path().to_path_buf(),
                additional_directories: Vec::new(),
                ephemeral: true,
                title: Some("Ephemeral memory settings".to_string()),
                model: Some("test-model".to_string()),
                model_binding_id: None,
            },
            /*tool_registry*/ None,
        )
        .await;
    let start_result = serde_json::from_value::<crate::SuccessResponse<crate::SessionStartResult>>(
        start_response,
    )?
    .result;
    let session_id = start_result.session.session_id;

    let recall_response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 101,
                "method": "session/metadata/update",
                "params": {
                    "sessionId": session_id,
                    "expectedVersion": 0,
                    "settings": { "memoryRecall": "off" }
                }
            }),
        )
        .await
        .context("ephemeral memory recall update response")?;
    let recall_result = serde_json::from_value::<
        crate::SuccessResponse<devo_protocol::native::rpc_session::SessionMetadataUpdateResult>,
    >(recall_response)?
    .result;
    assert_eq!(recall_result.session.version, 2);
    assert_eq!(
        recall_result.session.settings.memory_recall,
        MemorySetting::Off
    );
    assert_eq!(
        recall_result.session.settings.memory_contribution,
        MemorySetting::Inherit
    );

    let contribution_response = runtime
        .handle_incoming(
            connection_id,
            serde_json::json!({
                "id": 102,
                "method": "session/metadata/update",
                "params": {
                    "sessionId": session_id,
                    "expectedVersion": 2,
                    "settings": { "memoryContribution": "off" }
                }
            }),
        )
        .await
        .context("ephemeral memory contribution update response")?;
    let contribution_result = serde_json::from_value::<
        crate::SuccessResponse<devo_protocol::native::rpc_session::SessionMetadataUpdateResult>,
    >(contribution_response)?
    .result;
    assert_eq!(contribution_result.session.version, 3);
    assert_eq!(
        contribution_result.session.settings.memory_recall,
        MemorySetting::Off
    );
    assert_eq!(
        contribution_result.session.settings.memory_contribution,
        MemorySetting::Off
    );
    Ok(())
}
