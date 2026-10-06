use super::*;
use pretty_assertions::assert_eq;

/// Trace: L2-DES-MEM-001 DD-7.
/// Verifies: forks preserve inherited facts through restart and resume, while new external use leaves a clean original unmarked.
#[tokio::test]
async fn user_fork_inherits_external_fact_after_restart_and_resume() -> Result<()> {
    for initially_marked in [true, false] {
        let root = TempDir::new()?;
        let runtime = build_runtime(root.path());
        let connection = initialized_connection(&runtime).await;
        let source = start_durable_session(&runtime, connection, root.path()).await?;
        let source_record = runtime
            .session(source)
            .await
            .unwrap()
            .record()
            .await
            .flatten()
            .unwrap();
        if initially_marked {
            runtime
                .mark_external_context_used(
                    Some(source_record.rollout_path.clone()),
                    source,
                    /*parent_session_id*/ None,
                )
                .await
                .map_err(anyhow::Error::msg)?;
        }
        let fork = runtime
            .handle_incoming(
                connection,
                serde_json::json!({"id":120,"method":"session/fork","params":{"sessionId":source}}),
            )
            .await
            .context("fork response")?;
        let fork_id = SessionId::try_from(
            fork["result"]["session"]["id"]
                .as_str()
                .context("fork id")?,
        )?;
        let fork_record = runtime
            .session(fork_id)
            .await
            .unwrap()
            .record()
            .await
            .flatten()
            .unwrap();
        assert_eq!(
            external_context_fact_count(&fork_record.rollout_path)?,
            usize::from(initially_marked)
        );

        let restarted = build_runtime(root.path());
        restarted.load_persisted_sessions().await?;
        let connection = initialized_connection(&restarted).await;
        let resumed = restarted
            .handle_incoming(
                connection,
                serde_json::json!({"id":121,"method":"session/resume","params":{"sessionId":fork_id}}),
            )
            .await
            .context("resume response")?;
        assert!(resumed.get("result").is_some(), "{resumed}");
        restarted
            .mark_external_context_used(
                Some(fork_record.rollout_path.clone()),
                fork_id,
                /*parent_session_id*/ None,
            )
            .await
            .map_err(anyhow::Error::msg)?;
        assert_eq!(
            (
                external_context_fact_count(&source_record.rollout_path)?,
                external_context_fact_count(&fork_record.rollout_path)?
            ),
            (usize::from(initially_marked), 1)
        );
    }
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-7.
/// Verifies: a failed durable ancestor fact stops external publication even when optional memory is unavailable.
#[tokio::test]
async fn failed_ancestor_fact_rejects_external_publication_without_memory() -> Result<()> {
    let root = TempDir::new()?;
    std::fs::write(root.path().join("memory"), "unavailable")?;
    let runtime = build_runtime(root.path());
    assert!(runtime.memory.is_none());
    let connection = initialized_connection(&runtime).await;
    let parent = start_durable_session(&runtime, connection, root.path()).await?;
    let record = runtime
        .session(parent)
        .await
        .unwrap()
        .record()
        .await
        .flatten()
        .unwrap();
    let backup = root.path().join("rollout-backup.jsonl");
    std::fs::rename(&record.rollout_path, &backup)?;
    std::fs::create_dir(&record.rollout_path)?;
    let marked = runtime
        .mark_external_context_used(/*rollout_path*/ None, SessionId::new(), Some(parent))
        .await;
    std::fs::remove_dir(&record.rollout_path)?;
    std::fs::rename(&backup, &record.rollout_path)?;
    assert!(
        marked.is_err(),
        "external context must not escape an uncommitted ancestor fact"
    );
    Ok(())
}
