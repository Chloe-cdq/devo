use super::*;
use devo_protocol::PermissionPreset;
use pretty_assertions::assert_eq;

/// Trace: L2-DES-CONV-002 Rev 2 DD-3, L2-DES-SERVER-002.
/// Verifies: simultaneous producer tasks agree with the independently drained
/// actor, regardless of which valid mailbox order wins the race.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_permission_writers_match_the_drained_actor() -> Result<()> {
    let data_root = TempDir::new()?;
    let runtime = build_runtime(data_root.path());
    let connection_id = initialized_connection(&runtime).await;
    let session_id = start_durable_session(&runtime, connection_id, data_root.path()).await?;
    let handle = runtime.session(session_id).await.context("actor")?;
    let record = handle.record().await.flatten().context("record")?;
    for _ in 0..24 {
        let restored = runtime
            .hydrate_runtime_session(session_id, &record.rollout_path)
            .await?;
        let mut restored =
            crate::runtime::session_actor::SessionActorState::from_runtime_session(restored);
        restored.summary.permission_preset = Some(PermissionPreset::Default);
        let mut summary = restored.summary.clone();
        summary.permission_preset = Some(PermissionPreset::AutoReview);
        let full_profile = crate::runtime::safety_profile_from_protocol(
            PermissionPreset::FullAccess,
            data_root.path().to_path_buf(),
            Vec::new(),
        );
        let default_profile = crate::runtime::safety_profile_from_protocol(
            PermissionPreset::Default,
            data_root.path().to_path_buf(),
            Vec::new(),
        );
        let barrier = Arc::new(tokio::sync::Barrier::new(4));
        let replacement_handle = handle.clone();
        let replacement_barrier = Arc::clone(&barrier);
        let replacement = tokio::spawn(async move {
            replacement_barrier.wait().await;
            replacement_handle.replace_state(restored).await;
        });
        let summary_handle = handle.clone();
        let summary_barrier = Arc::clone(&barrier);
        let updating = tokio::spawn(async move {
            summary_barrier.wait().await;
            summary_handle.update_summary(summary).await;
        });
        let applying_handle = handle.clone();
        let applying_barrier = Arc::clone(&barrier);
        let applying = tokio::spawn(async move {
            applying_barrier.wait().await;
            applying_handle.apply_permission_profile(full_profile).await
        });
        let notifying_handle = handle.clone();
        let notifying = tokio::spawn(async move {
            barrier.wait().await;
            notifying_handle.notify_permission_profile(default_profile, "workspace".to_string());
        });
        replacement.await?;
        updating.await?;
        assert!(applying.await?);
        notifying.await?;
        let drained = handle.summary().await.context("drained actor summary")?;
        assert_eq!(handle.permission_preset(), drained.permission_preset);
    }
    Ok(())
}

/// Trace: L2-DES-CONV-002 Rev 2 DD-3/DD-6, L2-DES-SERVER-002.
/// Verifies: overlapping writers publish the accepted mailbox target in order;
/// a following Native patch to the same durable preset preserves its explicit sandbox.
#[tokio::test]
async fn permission_writers_preserve_mailbox_order_and_explicit_sandbox() -> Result<()> {
    let data_root = TempDir::new()?;
    let runtime = build_runtime(data_root.path());
    let connection_id = initialized_connection(&runtime).await;
    let session_id = start_durable_session(&runtime, connection_id, data_root.path()).await?;
    let handle = runtime.session(session_id).await.context("actor")?;
    let initial = history_request(
        &runtime,
        connection_id,
        119,
        "session/metadata/update",
        serde_json::json!({ "sessionId": session_id, "expectedVersion": 0,
            "settings": { "permissionProfile": "fullAccess", "sandboxProfile": "read-only" } }),
    )
    .await;
    anyhow::ensure!(initial.get("result").is_some(), "initial patch: {initial}");
    let record = handle.record().await.flatten().context("record")?;
    let restored = runtime
        .hydrate_runtime_session(session_id, &record.rollout_path)
        .await?;
    let mut restored =
        crate::runtime::session_actor::SessionActorState::from_runtime_session(restored);
    restored.summary.permission_preset = Some(PermissionPreset::AutoReview);
    let mut summary = restored.summary.clone();
    summary.permission_preset = Some(PermissionPreset::Default);

    // On this current-thread executor, explicit polling enqueues commands but
    // cannot let the actor acknowledge them. Writers therefore overlap without
    // scheduler sleeps; expectations follow the independently specified order.
    let replacement_writer = handle.clone();
    let replacement = replacement_writer.replace_state(restored);
    tokio::pin!(replacement);
    assert!(futures::poll!(&mut replacement).is_pending());
    let mut accepted = vec![handle.permission_preset()];
    let summary_writer = handle.clone();
    summary_writer.update_summary(summary).await;
    accepted.push(handle.permission_preset());
    let notification_writer = handle.clone();
    notification_writer.notify_permission_profile(
        crate::runtime::safety_profile_from_protocol(
            PermissionPreset::FullAccess,
            data_root.path().to_path_buf(),
            Vec::new(),
        ),
        "off".to_string(),
    );
    accepted.push(handle.permission_preset());
    let reply_writer = handle.clone();
    let applying =
        reply_writer.apply_permission_profile(crate::runtime::safety_profile_from_protocol(
            PermissionPreset::Default,
            data_root.path().to_path_buf(),
            Vec::new(),
        ));
    tokio::pin!(applying);
    assert!(futures::poll!(&mut applying).is_pending());
    accepted.push(handle.permission_preset());
    notification_writer.notify_permission_profile(
        crate::runtime::safety_profile_from_protocol(
            PermissionPreset::FullAccess,
            data_root.path().to_path_buf(),
            Vec::new(),
        ),
        "off".to_string(),
    );
    accepted.push(handle.permission_preset());
    assert_eq!(
        accepted,
        vec![
            Some(PermissionPreset::AutoReview),
            Some(PermissionPreset::Default),
            Some(PermissionPreset::FullAccess),
            Some(PermissionPreset::Default),
            Some(PermissionPreset::FullAccess)
        ]
    );
    handle.notify_sandbox_profile("read-only".to_string());
    let patch = history_request(
        &runtime,
        connection_id,
        120,
        "session/metadata/update",
        serde_json::json!({ "sessionId": session_id, "expectedVersion": 0,
            "settings": { "permissionProfile": "fullAccess", "memoryRecall": "off" } }),
    )
    .await;
    anyhow::ensure!(patch.get("result").is_some(), "patch: {patch}");
    let ((), applied) = tokio::join!(replacement, applying);
    assert!(applied);
    let actual_summary = handle.summary().await.context("summary after drain")?;
    let context = handle
        .hook_context_snapshot()
        .await
        .context("context after drain")?;
    assert_eq!(
        (
            actual_summary.permission_preset,
            context.config.sandbox_profile,
            handle
                .memory_settings()
                .await
                .context("memory snapshot")?
                .settings
                .recall
        ),
        (
            Some(PermissionPreset::FullAccess),
            Some("read-only".to_string()),
            devo_protocol::native::session::MemorySetting::Off
        )
    );
    assert_eq!(handle.permission_preset(), actual_summary.permission_preset);
    Ok(())
}

/// Trace: L2-DES-CONV-002 Rev 2 DD-3.
/// Verifies: rejected writes cannot publish a permission target never accepted by the actor.
#[tokio::test]
async fn closed_actor_rejects_all_permission_snapshot_writers() -> Result<()> {
    let data_root = TempDir::new()?;
    let runtime = build_runtime(data_root.path());
    let connection_id = initialized_connection(&runtime).await;
    let session_id = start_durable_session(&runtime, connection_id, data_root.path()).await?;
    let handle = runtime.session(session_id).await.context("actor")?;
    let record = handle.record().await.flatten().context("record")?;
    let restored = runtime
        .hydrate_runtime_session(session_id, &record.rollout_path)
        .await?;
    let mut restored =
        crate::runtime::session_actor::SessionActorState::from_runtime_session(restored);
    let initial = handle.permission_preset();
    restored.summary.permission_preset = Some(PermissionPreset::FullAccess);
    let summary = restored.summary.clone();
    let profile = crate::runtime::safety_profile_from_protocol(
        PermissionPreset::FullAccess,
        data_root.path().to_path_buf(),
        Vec::new(),
    );
    handle.shutdown().await;
    handle.update_summary(summary).await;
    assert_eq!(handle.permission_preset(), initial);
    handle.replace_state(restored).await;
    assert_eq!(handle.permission_preset(), initial);
    assert!(!handle.apply_permission_profile(profile.clone()).await);
    assert_eq!(handle.permission_preset(), initial);
    handle.notify_permission_profile(profile, "off".to_string());
    assert_eq!(handle.permission_preset(), initial);
    Ok(())
}
