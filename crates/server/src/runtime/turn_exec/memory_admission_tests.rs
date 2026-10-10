use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use pretty_assertions::assert_eq;

use crate::memory_forget_runtime_support::{
    configured_data_root, remember, start_subscribed_session,
};
use crate::runtime::session_actor::state::TurnMemoryPreparation;
use crate::support::{
    ScriptedProvider, StreamScript, build_runtime_with_workspace_config, message_texts,
    start_turn_with_approval_policy, wait_for_stream_calls,
};

enum ParentPreparation {
    Complete,
    Abort,
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-6
/// Verifies: concurrent Native delegation in the admission window waits on the same
/// preparation lane as later admission/checkout publication and inherits the original recall.
#[tokio::test]
async fn native_delegation_before_snapshot_registration_shares_parent_preparation() -> Result<()> {
    exercise_admission(ParentPreparation::Complete).await
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-6
/// Verifies: Native delegation waiting during admission fails promptly if that turn aborts.
#[tokio::test]
async fn native_delegation_waiters_close_when_parent_admission_aborts() -> Result<()> {
    exercise_admission(ParentPreparation::Abort).await
}

async fn exercise_admission(outcome: ParentPreparation) -> Result<()> {
    let data = configured_data_root()?;
    let provider = Arc::new(ScriptedProvider::new([
        StreamScript::Pending,
        ScriptedProvider::completed("first child"),
        ScriptedProvider::completed("second child"),
    ]));
    let runtime = build_runtime_with_workspace_config(data.path(), provider.clone())?;
    let (connection, _notifications, parent) =
        start_subscribed_session(&runtime, data.path(), /*request_id*/ 200).await?;
    remember(&runtime, connection, /*request_id*/ 201, "Use tabs").await?;
    start_turn_with_approval_policy(&runtime, connection, parent, "Use tabs", Some("never"))
        .await?;
    wait_for_stream_calls(&provider, /*expected*/ 1).await?;
    let snapshot = runtime
        .active_spawn_snapshot_for_session(parent)
        .await
        .context("parent snapshot")?;
    let turn_id = snapshot.parent_active_turn_id.context("parent turn")?;
    let original = message_texts(&provider.requests()[0])
        .into_iter()
        .find(|text| text.contains("<advisory_memory>"))
        .context("original recall")?;
    // Recreate the admission boundary: the actor is active before its spawn
    // snapshot has been published. Keep no sender from the previous publication.
    drop(snapshot);
    runtime.clear_turn_spawn_snapshot(parent, turn_id).await;
    let first = runtime.handle_incoming(
        connection,
        serde_json::json!({
            "id": 202, "method": "task/start", "params": {
                "kind": "agent", "sessionId": parent,
                "input": [{"type": "text", "text": "Use tabs"}],
                "forkTurns": "none", "idempotencyKey": "admission-child-one"
            }
        }),
    );
    let second = runtime.handle_incoming(
        connection,
        serde_json::json!({
            "id": 203, "method": "task/start", "params": {
                "kind": "agent", "sessionId": parent,
                "input": [{"type": "text", "text": "Use tabs"}],
                "forkTurns": "none", "idempotencyKey": "admission-child-two"
            }
        }),
    );
    tokio::pin!(first, second);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(/*millis*/ 50),
            futures::future::join(&mut first, &mut second)
        )
        .await
        .is_err(),
        "Native delegation must wait for parent preparation during admission"
    );
    let handle = runtime.session(parent).await.context("parent actor")?;
    match outcome {
        ParentPreparation::Complete => {
            // Later admission/checkout publishes a fresh snapshot of the same turn.
            // It must preserve the lane already observed by both requests.
            let snapshot = handle
                .spawn_snapshot()
                .await
                .context("admission snapshot")?;
            runtime
                .register_turn_spawn_snapshot(parent, turn_id, Arc::new(snapshot))
                .await;
            let snapshot = runtime
                .active_spawn_snapshot_for_session(parent)
                .await
                .context("registered snapshot")?;
            snapshot
                .prepared_memory
                .send_replace(TurnMemoryPreparation::Ready(Some(Arc::from(
                    original.clone(),
                ))));
        }
        ParentPreparation::Abort => {
            assert_eq!(
                handle.clear_active_turn_if_matches(turn_id).await,
                Some(true)
            );
        }
    }
    let (first, second) = tokio::time::timeout(
        Duration::from_secs(/*secs*/ 5),
        futures::future::join(first, second),
    )
    .await?;
    for response in [first, second] {
        let response = response.context("Native task response")?;
        match outcome {
            ParentPreparation::Complete => {
                anyhow::ensure!(
                    response.get("result").is_some(),
                    "delegation failed: {response}"
                );
            }
            ParentPreparation::Abort => {
                anyhow::ensure!(
                    response.get("error").is_some(),
                    "delegation must fail: {response}"
                );
            }
        }
    }
    if let ParentPreparation::Complete = outcome {
        wait_for_stream_calls(&provider, /*expected*/ 3).await?;
        let inherited: Vec<_> = provider.requests()[1..]
            .iter()
            .map(|request| {
                message_texts(request)
                    .into_iter()
                    .find(|text| text.contains("<advisory_memory>"))
            })
            .collect();
        assert_eq!(inherited, vec![Some(original.clone()), Some(original)]);
    }
    runtime.shutdown().await;
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 Failure and Observability / DD-6.
/// Verifies: root and automation turns and ping survive a busy background store, then ordinary recall resumes.
#[tokio::test]
async fn memory_contention_keeps_turns_and_ping_available() -> Result<()> {
    let data = configured_data_root()?;
    let provider = Arc::new(ScriptedProvider::new([
        ScriptedProvider::completed("root"),
        ScriptedProvider::completed("automation"),
    ]));
    let runtime = build_runtime_with_workspace_config(data.path(), provider.clone())?;
    let (connection, mut notifications, root) =
        start_subscribed_session(&runtime, data.path(), /*request_id*/ 210).await?;
    remember(&runtime, connection, /*request_id*/ 211, "Use tabs").await?;
    let created = runtime
        .handle_incoming(
            connection,
            serde_json::json!({
                "id": 212, "method": "session/new", "params": {
                    "cwd": data.path(), "source": "automation", "idempotencyKey": "busy-automation"
                }
            }),
        )
        .await
        .context("automation")?;
    let automation = serde_json::from_value(created["result"]["session"]["id"].clone())?;
    runtime.handle_incoming(connection, serde_json::json!({
        "id": 213, "method": "subscription/create", "params": {
            "selectors": [{"kind": "session", "sessionId": automation}], "includeSnapshot": false
        }
    })).await.context("subscribe automation")?;
    let memory = runtime.memory.as_ref().context("memory")?;
    let (held, release, worker) =
        crate::memory::contention_test_support::hold_storage(Arc::clone(memory));
    held.await?;
    let result = tokio::time::timeout(Duration::from_secs(/*secs*/ 5), async {
        for session in [root, automation] {
            start_turn_with_approval_policy(
                &runtime,
                connection,
                session,
                "Use tabs",
                Some("never"),
            )
            .await?;
            let completed = crate::support::wait_for_session_notification(
                &mut notifications,
                "turn/completed",
                session,
            )
            .await?;
            assert_eq!(
                completed["params"]["turn"]["status"],
                serde_json::json!("completed")
            );
        }
        let ping = runtime
            .handle_incoming(
                connection,
                serde_json::json!({
                    "id": 214, "method": "runtime/ping", "params": {}
                }),
            )
            .await
            .context("ping")?;
        anyhow::ensure!(ping.get("result").is_some(), "ping failed: {ping}");
        Ok::<_, anyhow::Error>(())
    })
    .await;
    let _ = release.send(());
    worker.join().unwrap();
    result.context("foreground must not queue behind memory storage")??;
    for request in provider.requests() {
        assert!(
            message_texts(&request)
                .iter()
                .all(|text| !text.contains("<advisory_memory>"))
        );
    }
    // Startup maintenance queued behind the holder may finish after its release.
    tokio::time::timeout(Duration::from_secs(/*secs*/ 5), async {
        loop {
            provider.push_scripts([ScriptedProvider::completed("recall resumed")]);
            start_turn_with_approval_policy(&runtime, connection, root, "Use tabs", Some("never"))
                .await?;
            crate::support::wait_for_session_notification(
                &mut notifications,
                "turn/completed",
                root,
            )
            .await?;
            if message_texts(provider.requests().last().context("resumed recall")?)
                .iter()
                .any(|text| text.contains("<advisory_memory>"))
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("recall must resume after storage contention")??;
    runtime.shutdown().await;
    Ok(())
}
