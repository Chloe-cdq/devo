use super::*;
use crate::runtime::turn_exec::journal::RolloutToolJournal;
use devo_core::durable_execution::{
    ExecutionRecord, RecoveryDisposition, RecoveryState, ToolIntentJournal,
};
use devo_core::{InternalRecordV2, RolloutLineV2};
use pretty_assertions::assert_eq;

/// Trace: L2-DES-CONTEXT-004, L2-DES-SERVER-002.
/// Verifies: a live execution replay waits for an in-progress append and sees
/// its complete recovery decision instead of silently discarding a partial tail.
#[tokio::test]
async fn execution_journal_replay_waits_for_complete_rollout_append() -> Result<()> {
    let dir = TempDir::new()?;
    let runtime = build_runtime(dir.path());
    let session_id = SessionId::new();
    let turn_id = TurnId::new();
    let path = dir.path().join("rollout.jsonl");
    let recovery = RecoveryState {
        revision: 2,
        attempt: 1,
        disposition: RecoveryDisposition::Canceled,
        reason: "Stopped by user".to_string(),
        idempotency_key: None,
    };
    let line = RolloutLineV2::Internal {
        v: 2,
        timestamp: chrono::Utc::now(),
        session_id: devo_protocol::native::ids::SessionId::from_legacy_uuid(session_id.into()),
        turn_id: Some(devo_protocol::native::ids::TurnId::from_legacy_uuid(
            turn_id.into(),
        )),
        seq: 0,
        entry: InternalRecordV2::Execution {
            record: ExecutionRecord::Recovery {
                state: recovery.clone(),
            },
        },
    };
    let (entered, release, writer) =
        crate::persistence::pause_rollout_append(runtime.rollout_store.clone(), path.clone(), line);
    entered.recv()?;
    let journal = RolloutToolJournal::new(runtime, path, session_id, turn_id);
    let mut reader = tokio::spawn(async move { journal.replay().await });
    let premature = tokio::time::timeout(Duration::from_millis(100), &mut reader).await;
    release.send(())?;
    writer.join().expect("writer thread");
    let (finished_early, result) = match premature {
        Ok(result) => (true, result??),
        Err(_) => (false, reader.await??),
    };
    assert_eq!((finished_early, result.recovery), (false, Some(recovery)));
    Ok(())
}

/// Trace: L2-DES-CONTEXT-004.
/// Verifies: cold journal initialization retains a concurrent recovery record,
/// so committing a checkpoint cannot admit an older recovery revision.
#[tokio::test]
async fn execution_journal_initial_commit_preserves_concurrent_recovery() -> Result<()> {
    let dir = TempDir::new()?;
    let runtime = build_runtime(dir.path());
    let session_id = SessionId::new();
    let turn_id = TurnId::new();
    let path = dir.path().join("rollout.jsonl");
    let recovery = RecoveryState {
        revision: 2,
        attempt: 1,
        disposition: RecoveryDisposition::Canceled,
        reason: "Stopped by user".to_string(),
        idempotency_key: None,
    };
    let line = RolloutLineV2::Internal {
        v: 2,
        timestamp: chrono::Utc::now(),
        session_id: devo_protocol::native::ids::SessionId::from_legacy_uuid(session_id.into()),
        turn_id: Some(devo_protocol::native::ids::TurnId::from_legacy_uuid(
            turn_id.into(),
        )),
        seq: 0,
        entry: InternalRecordV2::Execution {
            record: ExecutionRecord::Recovery {
                state: recovery.clone(),
            },
        },
    };
    let (entered, release, writer) =
        crate::persistence::pause_rollout_append(runtime.rollout_store.clone(), path.clone(), line);
    entered.recv()?;
    let journal = Arc::new(RolloutToolJournal::new(runtime, path, session_id, turn_id));
    let committing = Arc::clone(&journal);
    let mut checkpoint = tokio::spawn(async move {
        committing
            .commit(ExecutionRecord::PromptCheckpoint {
                items: Vec::new(),
                counters: None,
            })
            .await
    });
    let premature = tokio::time::timeout(Duration::from_millis(100), &mut checkpoint).await;
    release.send(())?;
    writer.join().expect("writer thread");
    match premature {
        Ok(result) => result??,
        Err(_) => checkpoint.await??,
    }
    let stale = journal
        .commit(ExecutionRecord::Recovery {
            state: RecoveryState {
                revision: 1,
                ..recovery
            },
        })
        .await;
    assert_eq!(
        stale.err().map(|error| error.to_string()),
        Some("conflicting recovery revision".to_string())
    );
    Ok(())
}
