use super::*;
use devo_protocol::native::session::MemorySetting;
use pretty_assertions::assert_eq;

/// Trace: L2-DES-CONV-002 Rev 2 DD-3, L2-DES-MEM-001 Rev 3 Session Controls.
/// Verifies: a patch queued behind hydration updates the newly registered actor.
#[tokio::test]
async fn memory_patch_waiting_for_resume_updates_the_resumed_actor() -> Result<()> {
    let data_root = TempDir::new()?;
    let runtime = build_runtime(data_root.path());
    let connection_id = initialized_connection(&runtime).await;
    let session_id = start_durable_session(&runtime, connection_id, data_root.path()).await?;
    let old_handle = runtime
        .remove_session_actor(session_id)
        .await
        .context("old actor")?;
    let record = old_handle.record().await.flatten().context("record")?;
    old_handle.shutdown().await;

    // Pause resume inside its real metadata gate, before registering its actor.
    let resume_permit = runtime
        .session_metadata_write_gate
        .acquire(session_id)
        .await;
    let restored = runtime
        .hydrate_runtime_session(session_id, &record.rollout_path)
        .await?;
    let patch = runtime.handle_incoming(
        connection_id,
        serde_json::json!({
            "id": 110, "method": "session/metadata/update",
            "params": { "sessionId": session_id, "expectedVersion": 0,
                "settings": { "memoryRecall": "off", "memoryContribution": "on" } }
        }),
    );
    tokio::pin!(patch);
    assert!(futures::poll!(&mut patch).is_pending());
    let handle = runtime.insert_root_session_actor(restored).await?;
    drop(resume_permit);
    let response = patch.await.context("patch response")?;
    let response: crate::SuccessResponse<
        devo_protocol::native::rpc_session::SessionMetadataUpdateResult,
    > = serde_json::from_value(response)?;
    let expected = crate::memory::SessionMemorySettings {
        recall: MemorySetting::Off,
        contribution: MemorySetting::On,
    };
    assert_eq!(
        handle
            .memory_settings()
            .await
            .context("live settings")?
            .settings,
        expected
    );
    let canonical = devo_core::read_canonical_history(&record.rollout_path)?;
    assert_eq!(
        canonical.session.context("canonical session")?.settings,
        response.result.session.settings
    );
    let fork = history_request(
        &runtime,
        connection_id,
        111,
        "session/fork",
        serde_json::json!({ "sessionId": session_id }),
    )
    .await;
    let fork: devo_protocol::native::rpc_session::SessionForkResult =
        serde_json::from_value(fork["result"].clone())?;
    assert_eq!(
        (
            fork.session.settings.memory_recall,
            fork.session.settings.memory_contribution
        ),
        (MemorySetting::Off, MemorySetting::On)
    );
    Ok(())
}

/// Trace: L2-DES-CONV-002 Rev 2 DD-2/DD-6
/// Verifies: a permission switch re-implies sandbox unless the patch explicitly retains the prior override.
#[tokio::test]
async fn permission_switch_replays_explicit_and_implied_sandbox_consistently() -> Result<()> {
    use devo_protocol::native::model::PermissionProfile;
    for (explicit_sandbox, effective_sandbox) in [(None, "off"), (Some("read-only"), "read-only")] {
        let data_root = TempDir::new()?;
        let runtime = build_runtime(data_root.path());
        let connection_id = initialized_connection(&runtime).await;
        let session_id = start_durable_session(&runtime, connection_id, data_root.path()).await?;
        let handle = runtime.session(session_id).await.context("actor")?;
        let record = handle.record().await.flatten().context("record")?;
        let first = history_request(
            &runtime,
            connection_id,
            200,
            "session/metadata/update",
            serde_json::json!({ "sessionId": session_id, "expectedVersion": 0,
                "settings": { "sandboxProfile": "read-only", "memoryRecall": "off" } }),
        )
        .await;
        let first: devo_protocol::native::rpc_session::SessionMetadataUpdateResult =
            serde_json::from_value(first["result"].clone())?;
        let switched = history_request(
            &runtime, connection_id, 201, "session/metadata/update",
            serde_json::json!({ "sessionId": session_id, "expectedVersion": 0,
                "settings": { "permissionProfile": "fullAccess", "sandboxProfile": explicit_sandbox } }),
        ).await;
        let switched: devo_protocol::native::rpc_session::SessionMetadataUpdateResult =
            serde_json::from_value(switched["result"].clone())?;
        assert_eq!(
            switched.session.settings,
            devo_protocol::native::session::SessionSettings {
                permission_profile: PermissionProfile::FullAccess,
                sandbox_profile: explicit_sandbox.map(str::to_string),
                ..first.session.settings
            }
        );
        let restored = runtime
            .hydrate_runtime_session(session_id, &record.rollout_path)
            .await?;
        assert_eq!(
            (
                handle
                    .hook_context_snapshot()
                    .await
                    .context("live config")?
                    .config
                    .sandbox_profile,
                restored
                    .core_session
                    .lock()
                    .await
                    .config
                    .sandbox_profile
                    .clone()
            ),
            (
                Some(effective_sandbox.to_string()),
                Some(effective_sandbox.to_string())
            )
        );
    }
    Ok(())
}

/// Trace: L2-DES-CONV-002 Rev 2 DD-3/DD-6, L2-DES-MEM-001 Rev 4 Session Controls.
/// Verifies: a real fork whose live preset already matches a durable change still re-implies sandbox.
#[tokio::test]
async fn fork_permission_change_reimplies_sandbox_when_live_preset_matches() -> Result<()> {
    use devo_protocol::native::model::PermissionProfile;
    let data_root = TempDir::new()?;
    let runtime = build_runtime(data_root.path());
    let connection_id = initialized_connection(&runtime).await;
    let session_id = start_durable_session(&runtime, connection_id, data_root.path()).await?;
    let parent = history_request(
        &runtime,
        connection_id,
        210,
        "session/metadata/update",
        serde_json::json!({ "sessionId": session_id, "expectedVersion": 0,
            "settings": { "permissionProfile": "fullAccess", "sandboxProfile": "read-only",
                "memoryContribution": "off" } }),
    )
    .await;
    anyhow::ensure!(parent.get("result").is_some(), "parent patch: {parent}");
    let forked = history_request(
        &runtime,
        connection_id,
        211,
        "session/fork",
        serde_json::json!({ "sessionId": session_id }),
    )
    .await;
    let forked: devo_protocol::native::rpc_session::SessionForkResult =
        serde_json::from_value(forked["result"].clone())?;
    let fork_id = SessionId::try_from(forked.session.id.as_str())?;
    let handle = runtime.session(fork_id).await.context("fork actor")?;
    let record = handle.record().await.flatten().context("fork record")?;
    assert_eq!(
        (
            handle.permission_preset(),
            handle
                .hook_context_snapshot()
                .await
                .context("inherited config")?
                .config
                .sandbox_profile,
            forked.session.settings.permission_profile
        ),
        (
            Some(devo_protocol::PermissionPreset::FullAccess),
            Some("read-only".to_string()),
            PermissionProfile::Default
        )
    );
    let patched = history_request(
        &runtime,
        connection_id,
        212,
        "session/metadata/update",
        serde_json::json!({ "sessionId": fork_id, "expectedVersion": 0,
            "settings": { "permissionProfile": "fullAccess", "memoryRecall": "off" } }),
    )
    .await;
    let patched: devo_protocol::native::rpc_session::SessionMetadataUpdateResult =
        serde_json::from_value(patched["result"].clone())?;
    let expected_settings = devo_protocol::native::session::SessionSettings {
        permission_profile: PermissionProfile::FullAccess,
        sandbox_profile: None,
        memory_recall: MemorySetting::Off,
        ..forked.session.settings
    };
    assert_eq!(patched.session.settings, expected_settings);
    assert_eq!(
        devo_core::read_canonical_history(&record.rollout_path)?
            .session
            .context("canonical fork")?
            .settings,
        expected_settings
    );
    let restored = runtime
        .hydrate_runtime_session(fork_id, &record.rollout_path)
        .await?;
    assert_eq!(
        (
            handle
                .hook_context_snapshot()
                .await
                .context("updated config")?
                .config
                .sandbox_profile,
            restored
                .core_session
                .lock()
                .await
                .config
                .sandbox_profile
                .clone()
        ),
        (Some("off".to_string()), Some("off".to_string()))
    );
    Ok(())
}

/// Trace: L2-DES-CONV-002 Rev 2 DD-3/DD-6.
/// Verifies: a mixed patch can tighten a fork whose live permission differs from its rollout.
#[tokio::test]
async fn memory_patch_can_tighten_fork_permission() -> Result<()> {
    let data_root = TempDir::new()?;
    let runtime = build_runtime(data_root.path());
    let connection_id = initialized_connection(&runtime).await;
    let session_id = start_durable_session(&runtime, connection_id, data_root.path()).await?;
    let changed = history_request(
        &runtime,
        connection_id,
        100,
        "session/metadata/update",
        serde_json::json!({ "sessionId": session_id, "expectedVersion": 0,
            "settings": { "permissionProfile": "fullAccess" } }),
    )
    .await;
    anyhow::ensure!(
        changed.get("result").is_some(),
        "initial permission patch: {changed}"
    );
    let forked = history_request(
        &runtime,
        connection_id,
        101,
        "session/fork",
        serde_json::json!({ "sessionId": session_id }),
    )
    .await;
    let forked: devo_protocol::native::rpc_session::SessionForkResult =
        serde_json::from_value(forked["result"].clone())?;
    let fork_id = SessionId::try_from(forked.session.id.as_str())?;
    let handle = runtime.session(fork_id).await.context("fork actor")?;
    assert_eq!(
        handle
            .summary()
            .await
            .context("fork summary")?
            .permission_preset,
        Some(devo_protocol::PermissionPreset::FullAccess)
    );

    let patched = history_request(
        &runtime,
        connection_id,
        102,
        "session/metadata/update",
        serde_json::json!({ "sessionId": fork_id, "expectedVersion": 0,
            "settings": { "permissionProfile": "default", "memoryRecall": "off" } }),
    )
    .await;
    let patched: devo_protocol::native::rpc_session::SessionMetadataUpdateResult =
        serde_json::from_value(patched["result"].clone())?;
    assert_eq!(
        patched.session.settings,
        devo_protocol::native::session::SessionSettings {
            permission_profile: devo_protocol::native::model::PermissionProfile::Default,
            memory_recall: MemorySetting::Off,
            ..forked.session.settings
        }
    );
    assert_eq!(
        handle
            .summary()
            .await
            .context("updated fork summary")?
            .permission_preset,
        Some(devo_protocol::PermissionPreset::Default)
    );
    Ok(())
}

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

/// Trace: L2-DES-CONV-002 Rev 2 DD-3/DD-6, L2-DES-MEM-001 Rev 4 Session Controls
/// Verifies: mixed durable patches preserve explicit sandbox across mailbox backpressure and cold recovery.
#[tokio::test]
async fn durable_memory_patch_does_not_wait_for_actor_reply() -> Result<()> {
    enum MailboxCapacity {
        Available,
        Full,
        OneSlot,
    }
    use devo_protocol::native::model::PermissionProfile;
    for (permission_profile, capacity) in [
        (None, MailboxCapacity::Available),
        (Some(PermissionProfile::FullAccess), MailboxCapacity::Full),
        (
            Some(PermissionProfile::FullAccess),
            MailboxCapacity::OneSlot,
        ),
        (
            Some(PermissionProfile::FullAccess),
            MailboxCapacity::Available,
        ),
    ] {
        let data_root = TempDir::new()?;
        let runtime = build_runtime(data_root.path());
        let connection_id = initialized_connection(&runtime).await;
        let session_id = start_durable_session(&runtime, connection_id, data_root.path()).await?;
        let handle = runtime.session(session_id).await.context("session actor")?;
        let record = handle.record().await.flatten().context("durable record")?;
        let initial_settings = devo_core::read_canonical_history(&record.rollout_path)?
            .session
            .context("initial canonical session")?
            .settings
            .clone();

        // Checkout exposes the existing shared stream Arc. The synthetic turn is
        // never executed or persisted; no model call or background turn is needed.
        let turn = crate::turn::TurnMetadata {
            turn_id: TurnId::new(),
            session_id,
            sequence: 1,
            status: TurnStatus::Running,
            kind: devo_core::TurnKind::ManualCompaction,
            model: "test-model".to_string(),
            model_binding_id: None,
            reasoning_effort_selection: None,
            reasoning_effort: None,
            request_model: "test-model".to_string(),
            request_thinking: None,
            started_at: Utc::now(),
            completed_at: None,
            usage: None,
            stop_reason: None,
            failure_reason: None,
        };
        let working = handle
            .checkout_turn_working_set(turn)
            .await
            .context("checkout shared stream")?;
        let stream_guard = working.state.stream.lock().await;

        // This short mailbox command takes the shared stream lock. Poll once to
        // enqueue it before the metadata patch. FIFO ordering means any later
        // GetSummary must wait behind this command until stream_guard is dropped.
        // The lock, rather than an arbitrary sleep, establishes the obstruction.
        let blocked_snapshot = handle.take_shutdown_deferred_snapshot();
        tokio::pin!(blocked_snapshot);
        assert!(futures::poll!(&mut blocked_snapshot).is_pending());
        let initial_permission = handle.permission_preset();
        match capacity {
            MailboxCapacity::Available => {}
            MailboxCapacity::Full => while handle.try_touch_last_activity() {},
            MailboxCapacity::OneSlot => {
                // The actor mailbox has 64 slots, one occupied by the blocker.
                // Without yielding, leave exactly one slot for the mixed update.
                for _ in 0..62 {
                    assert!(handle.try_touch_last_activity());
                }
            }
        }

        let patched = tokio::time::timeout(
            Duration::from_secs(5),
            runtime.handle_incoming(
                connection_id,
                serde_json::json!({
                    "id": 101,
                    "method": "session/metadata/update",
                    "params": {
                        "sessionId": session_id,
                        "expectedVersion": 0,
                        "settings": {
                            "memoryRecall": "off",
                            "permissionProfile": permission_profile,
                            "sandboxProfile": permission_profile.map(|_| "read-only")
                        }
                    }
                }),
            ),
        )
        .await;

        if patched.is_ok() && matches!(capacity, MailboxCapacity::Full) {
            assert_eq!(handle.permission_preset(), initial_permission);
        }
        // A repeated durable preset must preserve the explicit sandbox even
        // when its first actor notification was rejected or is still queued.
        let repeated = if permission_profile.is_some() && patched.is_ok() {
            Some(
                tokio::time::timeout(
                    Duration::from_secs(5),
                    runtime.handle_incoming(
                        connection_id,
                        serde_json::json!({
                            "id": 102,
                            "method": "session/metadata/update",
                            "params": {
                                "sessionId": session_id,
                                "expectedVersion": 0,
                                "settings": {
                                    "permissionProfile": permission_profile,
                                    "memoryContribution": "on"
                                }
                            }
                        }),
                    ),
                )
                .await,
            )
        } else {
            None
        };

        // Always unblock the real actor before reporting a timeout/failure.
        // patched already completed or timed out while the actor was obstructed.
        drop(stream_guard);
        tokio::time::timeout(Duration::from_secs(5), &mut blocked_snapshot)
            .await
            .context("actor snapshot cleanup timeout")?
            .context("actor snapshot cleanup")?;
        let response = patched
            .context("durable memory patch waited for actor reply")?
            .context("metadata response")?;
        let response = serde_json::from_value::<
            crate::SuccessResponse<devo_protocol::native::rpc_session::SessionMetadataUpdateResult>,
        >(response)?;

        let expected_settings = devo_protocol::native::session::SessionSettings {
            permission_profile: permission_profile.unwrap_or(initial_settings.permission_profile),
            sandbox_profile: if permission_profile.is_some() {
                Some("read-only".to_string())
            } else {
                initial_settings.sandbox_profile.clone()
            },
            memory_recall: MemorySetting::Off,
            ..initial_settings
        };
        assert_eq!(response.result.session.settings, expected_settings);
        let expected_settings = if let Some(repeated) = repeated {
            let repeated = repeated
                .context("repeated patch waited for actor")?
                .context("repeated patch response")?;
            let repeated: crate::SuccessResponse<
                devo_protocol::native::rpc_session::SessionMetadataUpdateResult,
            > = serde_json::from_value(repeated)?;
            let expected = devo_protocol::native::session::SessionSettings {
                memory_contribution: MemorySetting::On,
                ..expected_settings
            };
            assert_eq!(repeated.result.session.settings, expected);
            expected
        } else {
            expected_settings
        };
        let persisted = devo_core::read_canonical_history(&record.rollout_path)?
            .session
            .context("persisted canonical session")?;
        assert_eq!(persisted.settings, expected_settings);
        if permission_profile.is_some() {
            let restored = runtime
                .hydrate_runtime_session(session_id, &record.rollout_path)
                .await?;
            assert_eq!(
                restored.core_session.lock().await.config.sandbox_profile,
                Some("read-only".to_string())
            );
            // The actor has drained; retrying the same target can now repair
            // live state, but must retain the durable explicit sandbox.
            let reconciled = history_request(
                &runtime,
                connection_id,
                103,
                "session/metadata/update",
                serde_json::json!({ "sessionId": session_id, "expectedVersion": 0,
                    "settings": { "permissionProfile": "fullAccess" } }),
            )
            .await;
            let reconciled: devo_protocol::native::rpc_session::SessionMetadataUpdateResult =
                serde_json::from_value(reconciled["result"].clone())?;
            assert_eq!(reconciled.session.settings, expected_settings);
            assert_eq!(
                handle
                    .hook_context_snapshot()
                    .await
                    .context("hook snapshot")?
                    .config
                    .sandbox_profile,
                Some("read-only".to_string())
            );
        }
    }
    Ok(())
}
