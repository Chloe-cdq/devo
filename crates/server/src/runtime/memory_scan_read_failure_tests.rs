use super::super::*;
use crate::memory::source_read_test_support::{ReadPoint, on_read};
use crate::memory::{MemoryCommand, MemoryCommandResult};
use devo_protocol::native::rpc_memory::MemoryStatus;
use pretty_assertions::assert_eq;

#[derive(Clone, Copy)]
enum ReadFailurePoint {
    BeforeDispatch,
    BeforeCommit,
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 Background Scheduling and Failure Policy.
/// Verifies: pre-dispatch and pre-commit source I/O failures expose safe status and preserve bounded retry eligibility.
#[tokio::test]
async fn transient_source_read_failures_are_visible_and_retryable() -> Result<()> {
    for point in [
        ReadFailurePoint::BeforeDispatch,
        ReadFailurePoint::BeforeCommit,
    ] {
        let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 0)?;
        let metadata = runtime.deps.db.list_root_sessions()?[0].clone();
        let path = runtime
            .deps
            .db
            .get_session_index(&metadata.session_id)?
            .context("source index")?
            .rollout_path
            .context("source journal")?;
        let saved = root.path().join("temporarily-unavailable-source.jsonl");
        let initial_calls = match point {
            ReadFailurePoint::BeforeDispatch => {
                let moved_path = path.clone();
                let moved_saved = saved.clone();
                let hook = on_read(&path, ReadPoint::Complete, /*skip_reads*/ 0, move || {
                    std::fs::rename(moved_path, moved_saved).unwrap();
                });
                let result = scan(&runtime, root.path()).await;
                std::fs::rename(&saved, &path)?;
                drop(hook);
                result?;
                0
            }
            ReadFailurePoint::BeforeCommit => {
                let worker_runtime = Arc::clone(&runtime);
                let worker_root = root.path().to_path_buf();
                let worker = tokio::spawn(async move { scan(&worker_runtime, &worker_root).await });
                tokio::time::timeout(Duration::from_secs(/*secs*/ 5), provider.entered.notified())
                    .await?;
                std::fs::rename(&path, &saved)?;
                provider.release.add_permits(/*n*/ 1);
                let result = worker.await?;
                std::fs::rename(&saved, &path)?;
                result?;
                1
            }
        };
        let memory = runtime.memory.as_ref().context("memory runtime")?;
        let MemoryCommandResult::Status(status) =
            memory.execute_command(MemoryCommand::Status).await?
        else {
            panic!("status result")
        };
        assert_eq!(
            status,
            MemoryStatus {
                enabled: true,
                storage_health: "degraded".into(),
                entry_count: 0,
                candidate_count: 0,
                pending_job_count: 0,
                retrying_job_count: 1,
                error_job_count: 0,
                last_successful_scan_at: None,
                error_classes: vec!["storage_error".into()],
                source_exclusion_reasons: Vec::new(),
                rebuild: None,
            }
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), initial_calls);
        let db = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
        let job = db.query_row(
            "SELECT state, attempt_count, error_class, lease_owner, lease_until, retry_at IS NOT NULL FROM memory_jobs",
            [], |row| Ok((row.get::<_, String>(/*idx*/ 0)?, row.get::<_, u32>(/*idx*/ 1)?, row.get::<_, Option<String>>(/*idx*/ 2)?,
                row.get::<_, Option<String>>(/*idx*/ 3)?, row.get::<_, Option<String>>(/*idx*/ 4)?, row.get::<_, bool>(/*idx*/ 5)?)))?;
        assert_eq!(
            job,
            (
                "retrying".into(),
                1,
                Some("storage_error".into()),
                None,
                None,
                true
            )
        );
        // Make the persisted backoff due without sleeping or changing process time.
        db.execute(
            "UPDATE memory_jobs SET retry_at = '2000-01-01T00:00:00Z'",
            [],
        )?;
        provider.release.add_permits(/*n*/ 1);
        scan(&runtime, root.path()).await?;
        let MemoryCommandResult::Status(status) =
            memory.execute_command(MemoryCommand::Status).await?
        else {
            panic!("status result")
        };
        assert_eq!(
            (
                status.storage_health,
                status.error_classes,
                status.entry_count,
                status.pending_job_count,
                status.retrying_job_count,
                status.error_job_count,
                provider.calls.load(Ordering::SeqCst)
            ),
            (
                "degraded".into(),
                vec!["storage_error".into()],
                1,
                0,
                0,
                0,
                initial_calls + 1
            )
        );
        assert!(status.last_successful_scan_at.is_some());
    }
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 Background Scheduling and Failure Policy.
/// Verifies: repeatedly failing source reads terminate after three attempts without dispatching extraction.
#[tokio::test]
async fn source_read_failures_exhaust_bounded_retries() -> Result<()> {
    let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 0)?;
    let metadata = runtime.deps.db.list_root_sessions()?[0].clone();
    let path = runtime
        .deps
        .db
        .get_session_index(&metadata.session_id)?
        .context("source index")?
        .rollout_path
        .context("source journal")?;
    let saved = root.path().join("temporarily-unavailable-source.jsonl");
    let db = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
    for attempt in 1..=3 {
        let moved_path = path.clone();
        let moved_saved = saved.clone();
        let hook = on_read(&path, ReadPoint::Complete, /*skip_reads*/ 0, move || {
            std::fs::rename(moved_path, moved_saved).unwrap();
        });
        let result = scan(&runtime, root.path()).await;
        std::fs::rename(&saved, &path)?;
        drop(hook);
        result?;
        let job = db.query_row(
            "SELECT state, attempt_count, error_class, lease_owner, lease_until FROM memory_jobs",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(/*idx*/ 0)?,
                    row.get::<_, u32>(/*idx*/ 1)?,
                    row.get::<_, Option<String>>(/*idx*/ 2)?,
                    row.get::<_, Option<String>>(/*idx*/ 3)?,
                    row.get::<_, Option<String>>(/*idx*/ 4)?,
                ))
            },
        )?;
        assert_eq!(
            job,
            (
                if attempt < 3 { "retrying" } else { "error" }.into(),
                attempt,
                Some("storage_error".into()),
                None,
                None
            )
        );
        db.execute(
            "UPDATE memory_jobs SET retry_at = '2000-01-01T00:00:00Z' WHERE state = 'retrying'",
            [],
        )?;
    }
    scan(&runtime, root.path()).await?;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-9, Background Scheduling and Failure Policy.
/// Verifies: a temporary missing rebuild source retains its retry job and explicit authorization until storage recovers.
#[tokio::test]
async fn rebuild_source_read_failures_preserve_retry_authorization() -> Result<()> {
    use crate::memory::{
        MemoryUserSessionSelection, ProjectMemorySession, ProjectMemorySessionActivity,
    };
    use devo_protocol::native::rpc_memory::MemoryScope;
    use devo_protocol::native::session::SessionSource;
    let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 0)?;
    let metadata = runtime.deps.db.list_root_sessions()?[0].clone();
    let path = runtime
        .deps
        .db
        .get_session_index(&metadata.session_id)?
        .context("source index")?
        .rollout_path
        .context("source journal")?;
    let memory = runtime.memory.as_ref().context("memory runtime")?;
    memory
        .execute_command(MemoryCommand::Rebuild {
            scope: MemoryScope::User,
            user_session: MemoryUserSessionSelection::Selected(metadata.session_id),
            sessions: vec![ProjectMemorySession {
                session_id: metadata.session_id,
                workspace_root: Some(root.path().to_path_buf()),
                activity: ProjectMemorySessionActivity::Inactive,
                source: Some(SessionSource::Interactive),
            }],
        })
        .await?;
    let saved = root.path().join("temporarily-unavailable-source.jsonl");
    let worker_runtime = Arc::clone(&runtime);
    let worker_root = root.path().to_path_buf();
    let worker = tokio::spawn(async move {
        scan_triggered(&worker_runtime, &worker_root, ScanTrigger::ExplicitRebuild).await
    });
    tokio::time::timeout(Duration::from_secs(/*secs*/ 5), provider.entered.notified()).await?;
    std::fs::rename(&path, &saved)?;
    provider.release.add_permits(/*n*/ 1);
    let result = worker.await?;
    std::fs::rename(&saved, &path)?;
    result?;
    let db = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
    let progress = db.query_row(
        "SELECT r.state, j.state, j.attempt_count, j.error_class FROM memory_rebuild_requests r JOIN memory_jobs j ON j.rebuild_id = r.rebuild_id",
        [], |row| Ok((row.get::<_, String>(/*idx*/ 0)?, row.get::<_, String>(/*idx*/ 1)?, row.get::<_, u32>(/*idx*/ 2)?, row.get::<_, Option<String>>(/*idx*/ 3)?)))?;
    assert_eq!(
        progress,
        (
            "pending".into(),
            "retrying".into(),
            1,
            Some("storage_error".into())
        )
    );
    db.execute(
        "UPDATE memory_jobs SET retry_at = '2000-01-01T00:00:00Z'",
        [],
    )?;
    provider.release.add_permits(/*n*/ 1);
    scan_triggered(&runtime, root.path(), ScanTrigger::ExplicitRebuild).await?;
    let progress = db.query_row(
        "SELECT r.state, j.state, j.attempt_count, j.error_class FROM memory_rebuild_requests r JOIN memory_jobs j ON j.rebuild_id = r.rebuild_id",
        [], |row| Ok((row.get::<_, String>(/*idx*/ 0)?, row.get::<_, String>(/*idx*/ 1)?, row.get::<_, u32>(/*idx*/ 2)?, row.get::<_, Option<String>>(/*idx*/ 3)?)))?;
    assert_eq!(progress, ("completed".into(), "completed".into(), 2, None));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    Ok(())
}
