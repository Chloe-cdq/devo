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
