use super::*;
use pretty_assertions::assert_eq;
use serde_json::{Value, json};

async fn rpc(
    runtime: &Arc<ServerRuntime>,
    connection: u64,
    method: &str,
    params: Value,
) -> Result<Value> {
    runtime
        .handle_incoming(connection, json!({"id":90,"method":method,"params":params}))
        .await
        .context("Native response")
}

async fn bind_source(runtime: &Arc<ServerRuntime>, connection: u64) -> Result<()> {
    let source = runtime.deps.db.list_root_sessions()?[0].session_id;
    let result = rpc(
        runtime,
        connection,
        "subscription/create",
        json!({
            "selectors":[{"kind":"session","sessionId":source}], "includeSnapshot":false
        }),
    )
    .await?;
    anyhow::ensure!(result.get("result").is_some(), "subscription: {result}");
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-9, DD-12. Rebuild must be a registered, explicitly scoped Native operation.
#[tokio::test]
async fn rebuild_requires_scope_and_interactive_binding() -> Result<()> {
    let (_root, runtime, _) = setup(/*sources*/ 1, /*permits*/ 0)?;
    let connection = connect(&runtime).await?;
    for params in [
        json!({}),
        json!({"scope":null}),
        json!({"scope":"all"}),
        json!({"scope":"user"}),
        json!({"scope":"project"}),
    ] {
        let response = rpc(&runtime, connection, "memory/rebuild", params).await?;
        assert_eq!(
            response["error"]["code"],
            json!("InvalidParams"),
            "{response}"
        );
        assert!(
            !response["error"]["message"]
                .as_str()
                .unwrap()
                .contains("unknown method")
        );
    }
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-9, DD-12. Reset/startup/ordinary scanning cannot replay history; explicit rebuild can.
#[tokio::test]
async fn rebuild_recovers_pre_reset_history_without_automatic_replay() -> Result<()> {
    let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 1)?;
    scan(&runtime, root.path()).await?;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let connection = connect(&runtime).await?;
    bind_source(&runtime, connection).await?;
    let reset = rpc(
        &runtime,
        connection,
        "memory/reset",
        json!({"scope":"user"}),
    )
    .await?;
    anyhow::ensure!(reset.get("result").is_some(), "reset: {reset}");
    drop(runtime);
    let runtime = open_scan_runtime(root.path(), Arc::clone(&provider))?;
    scan(&runtime, root.path()).await?;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    let connection = connect(&runtime).await?;
    bind_source(&runtime, connection).await?;
    let accepted = rpc(
        &runtime,
        connection,
        "memory/rebuild",
        json!({"scope":"user"}),
    )
    .await?;
    anyhow::ensure!(accepted.get("result").is_some(), "rebuild: {accepted}");
    tokio::time::timeout(Duration::from_secs(10), provider.entered.notified())
        .await
        .context("extractor entry notification")?;
    // Consume the initial scan's notification if it was still buffered.
    tokio::time::timeout(Duration::from_secs(10), async {
        while provider.calls.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .with_context(|| {
        format!(
            "rebuild not claimed, calls={}",
            provider.calls.load(Ordering::SeqCst)
        )
    })?;
    let status = rpc(&runtime, connection, "memory/status", json!({})).await?;
    assert_eq!(status["result"]["rebuild"]["runningJobCount"], json!(1));
    assert!(!status.to_string().contains("I prefer tabs"));
    let ping = tokio::time::timeout(
        Duration::from_secs(1),
        rpc(&runtime, connection, "runtime/ping", json!({})),
    )
    .await??;
    assert!(ping.get("result").is_some());
    let retry = rpc(
        &runtime,
        connection,
        "memory/rebuild",
        json!({"scope":"user"}),
    )
    .await?;
    assert_eq!(retry["result"], accepted["result"]);
    provider.release.add_permits(1);
    let committed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let list = rpc(&runtime, connection, "memory/list", json!({"scope":"user"}))
                .await
                .unwrap();
            if list["result"]["data"]
                .as_array()
                .is_some_and(|data| data.len() == 1)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    if committed.is_err() {
        let status = rpc(&runtime, connection, "memory/status", json!({})).await?;
        let list = rpc(&runtime, connection, "memory/list", json!({"scope":"user"})).await?;
        anyhow::bail!("rebuild entry not committed: status={status}, list={list}");
    }
    scan(&runtime, root.path()).await?;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    let source = runtime.deps.db.list_root_sessions()?[0].session_id;
    let path = runtime
        .deps
        .db
        .get_session_index(&source)?
        .unwrap()
        .rollout_path
        .unwrap();
    assert!(std::fs::read_to_string(path)?.contains("memoryExtraction"));
    let export = rpc(
        &runtime,
        connection,
        "memory/export",
        json!({"scope":"user"}),
    )
    .await?;
    assert!(
        export["result"]["markdown"]
            .as_str()
            .unwrap()
            .contains("I prefer tabs")
    );
    assert_eq!(
        export["result"]["lifecycle"]["ignoreSourcesBefore"],
        reset["result"]["ignoreSourcesBefore"]
    );
    assert!(
        export["result"]["lifecycle"]["lastRebuildAt"]
            .as_str()
            .is_some()
    );
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-7, DD-9. Deferred rebuild jobs must reapply every current source exclusion before processing.
#[tokio::test]
async fn rebuild_reevaluates_queued_source_eligibility() -> Result<()> {
    for case in [
        "external",
        "subagent",
        "ephemeral",
        "automation",
        "fork",
        "missing_path",
        "contribution_off",
        "malformed",
    ] {
        let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 1)?;
        *provider.quota.lock().unwrap() = Some(0);
        let connection = connect(&runtime).await?;
        bind_source(&runtime, connection).await?;
        let accepted = rpc(
            &runtime,
            connection,
            "memory/rebuild",
            json!({"scope":"user"}),
        )
        .await?;
        anyhow::ensure!(accepted.get("result").is_some(), "rebuild: {accepted}");
        scan(&runtime, root.path()).await?;
        let mut metadata = runtime.deps.db.list_root_sessions()?[0].clone();
        let path = runtime
            .deps
            .db
            .get_session_index(&metadata.session_id)?
            .unwrap()
            .rollout_path
            .unwrap();
        let mut lines = std::fs::read_to_string(&path)?
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        match case {
            "external" => runtime.rollout_store.mark_external_context_used_at(&path, metadata.session_id)?,
            "subagent" => {
                metadata.parent_session_id = Some(SessionId::new());
                metadata.agent_path = Some("root/child".into());
                lines[0]["SessionMeta"]["session"]["parent_session_id"] = json!(metadata.parent_session_id);
            }
            "ephemeral" => {
                metadata.ephemeral = true;
                // Persistence is immutable in the production metadata upsert.
                rusqlite::Connection::open(root.path().join("devo.db"))?.execute("UPDATE sessions SET ephemeral = 1 WHERE id = ?1", [metadata.session_id.to_string()])?;
            },
            "automation" => lines[0]["SessionMeta"]["session"]["source"] = json!("heartbeat"),
            "fork" => {
                metadata.fork_from_id = Some(SessionId::new());
                lines[0]["SessionMeta"]["session"]["fork_from_id"] = json!(metadata.fork_from_id);
            }
            "missing_path" => {
                rusqlite::Connection::open(root.path().join("devo.db"))?.execute("UPDATE sessions SET rollout_path = NULL WHERE id = ?1", [metadata.session_id.to_string()])?;
            }
            "contribution_off" => lines.push(json!({"v":2,"kind":"internal","timestamp":Utc::now()-chrono::Duration::hours(7),
                "sessionId":metadata.session_id,"turnId":null,"seq":0,"entry":{
                    "type":"sessionSettings","schemaVersion":1,"field":"memoryContribution","value":"off","epoch":1}})),
            "malformed" => { std::fs::write(&path, "invalid history\n")?; }
            _ => unreachable!("literal cases"),
        }
        if !matches!(case, "external" | "missing_path" | "malformed") {
            std::fs::write(
                &path,
                lines
                    .iter()
                    .map(serde_json::to_string)
                    .collect::<Result<Vec<_>, _>>()?
                    .join("\n")
                    + "\n",
            )?;
            runtime.deps.db.upsert_session(&metadata, Some(&path))?;
        }
        *provider.quota.lock().unwrap() = Some(100);
        scan(&runtime, root.path()).await?;
        let status = rpc(&runtime, connection, "memory/status", json!({})).await?;
        assert_eq!(provider.calls.load(Ordering::SeqCst), 0, "{case}");
        assert_eq!(status["result"]["entryCount"], json!(0), "{case}");
        assert_eq!(
            status["result"]["rebuild"]["pendingJobCount"],
            json!(0),
            "{case}: {status}"
        );
        assert_eq!(
            status["result"]["rebuild"]["pendingRequestCount"],
            json!(0),
            "{case}: {status}"
        );
        assert!(!status.to_string().contains("I prefer tabs"));
    }
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-9, Operational Scheduling. Accepted rebuilds survive restart and quota deferral with unspent attempts.
#[tokio::test]
async fn rebuild_pending_jobs_resume_after_restart_and_quota_recovery() -> Result<()> {
    let (root, runtime, provider) = setup(/*sources*/ 3, /*permits*/ 3)?;
    *provider.quota.lock().unwrap() = None;
    let connection = connect(&runtime).await?;
    bind_source(&runtime, connection).await?;
    let accepted = rpc(
        &runtime,
        connection,
        "memory/rebuild",
        json!({"scope":"user"}),
    )
    .await?;
    anyhow::ensure!(accepted.get("result").is_some(), "rebuild: {accepted}");
    scan(&runtime, root.path()).await?;
    let status = rpc(&runtime, connection, "memory/status", json!({})).await?;
    assert_eq!(status["result"]["rebuild"]["pendingJobCount"], json!(3));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    runtime.shutdown().await;
    drop(runtime);
    let runtime = open_scan_runtime(root.path(), Arc::clone(&provider))?;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    *provider.quota.lock().unwrap() = Some(25);
    scan(&runtime, root.path()).await?;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
    let connection = connect(&runtime).await?;
    let status = rpc(&runtime, connection, "memory/status", json!({})).await?;
    assert_eq!(
        status["result"]["rebuild"],
        json!({"pendingRequestCount":0,"pendingJobCount":0,"runningJobCount":0,
        "retryingJobCount":0,"completedJobCount":3,"errorJobCount":0})
    );
    Ok(())
}

async fn authorize(
    runtime: &Arc<ServerRuntime>,
    connection: u64,
    scope: devo_protocol::native::rpc_memory::MemoryScope,
) -> Result<()> {
    let context = runtime.memory_command_sessions(connection, &[]).await;
    runtime
        .memory
        .as_ref()
        .unwrap()
        .execute_command(crate::memory::MemoryCommand::Rebuild {
            scope,
            user_session: context.user_session,
            sessions: context.sessions,
        })
        .await?;
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-9. Reset during the pre-send journal reread cancels authorization before retained text is sent.
#[tokio::test]
async fn reset_during_rebuild_reread_prevents_provider_dispatch() -> Result<()> {
    use crate::memory::source_read_test_support::{ReadPoint, on_read};
    let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 1)?;
    let connection = connect(&runtime).await?;
    bind_source(&runtime, connection).await?;
    authorize(
        &runtime,
        connection,
        devo_protocol::native::rpc_memory::MemoryScope::User,
    )
    .await?;
    let source = runtime.deps.db.list_root_sessions()?[0].session_id;
    let path = runtime
        .deps
        .db
        .get_session_index(&source)?
        .unwrap()
        .rollout_path
        .unwrap();
    let memory = Arc::clone(runtime.memory.as_ref().unwrap());
    let handle = tokio::runtime::Handle::current();
    let _hook = on_read(&path, ReadPoint::Complete, /*skip_reads*/ 1, move || {
        handle
            .block_on(memory.execute_command(crate::memory::MemoryCommand::Reset(
                crate::memory::ScopedMemoryRequest {
                    scope: devo_protocol::native::rpc_memory::MemoryScope::User,
                    workspace_root: Default::default(),
                },
            )))
            .unwrap();
    });
    scan(&runtime, root.path()).await?;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    let status = rpc(&runtime, connection, "memory/status", json!({})).await?;
    assert_eq!(status["result"]["entryCount"], json!(0));
    Ok(())
}

struct ActiveSource(SessionId);
#[async_trait::async_trait]
impl crate::memory::scan::SourceActivity for ActiveSource {
    async fn is_active(&self, session: SessionId) -> bool {
        session == self.0
    }
}

/// Trace: L2-DES-MEM-001 DD-3, DD-9. Busy retained sources in another Project must not hold a scoped rebuild open.
#[tokio::test]
async fn project_rebuild_ignores_active_sources_in_other_projects() -> Result<()> {
    let (root, runtime, provider) = setup(/*sources*/ 2, /*permits*/ 1)?;
    let sources = runtime.deps.db.list_root_sessions()?;
    let other = root.path().join("other-project");
    std::fs::create_dir(&other)?;
    let mut metadata = sources[1].clone();
    let path = runtime
        .deps
        .db
        .get_session_index(&metadata.session_id)?
        .unwrap()
        .rollout_path
        .unwrap();
    let mut lines = std::fs::read_to_string(&path)?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    lines[0]["SessionMeta"]["session"]["cwd"] = json!(other);
    metadata.cwd = other;
    std::fs::write(
        &path,
        lines
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()?
            .join("\n")
            + "\n",
    )?;
    runtime.deps.db.upsert_session(&metadata, Some(&path))?;
    let connection = connect(&runtime).await?;
    let subscribed = rpc(&runtime, connection, "subscription/create", json!({
        "selectors":[{"kind":"session","sessionId":sources[0].session_id}],"includeSnapshot":false
    })).await?;
    anyhow::ensure!(
        subscribed.get("result").is_some(),
        "subscription: {subscribed}"
    );
    authorize(
        &runtime,
        connection,
        devo_protocol::native::rpc_memory::MemoryScope::Project,
    )
    .await?;
    Arc::clone(runtime.memory.as_ref().unwrap())
        .run_background_scan(crate::memory::scan::ScanContext {
            db: Arc::clone(&runtime.deps.db),
            model_context: runtime.deps.context_for_workspace(root.path()).await?,
            usage_ledger: runtime.usage_ledger.clone(),
            triggering_session: sources[0].session_id,
            activity: Arc::new(ActiveSource(metadata.session_id)),
        })
        .await?;
    let status = rpc(&runtime, connection, "memory/status", json!({})).await?;
    assert_eq!(status["result"]["rebuild"]["pendingRequestCount"], json!(0));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-9, DD-12. Fingerprint upgrades cannot replay a v10 completed source or invent rebuild authorization.
#[tokio::test]
async fn legacy_usage_receipt_upgrade_does_not_replay_or_rebuild() -> Result<()> {
    use sha2::{Digest, Sha256};
    let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 1)?;
    let source = runtime.deps.db.list_root_sessions()?[0].session_id;
    let path = runtime
        .deps
        .db
        .get_session_index(&source)?
        .unwrap()
        .rollout_path
        .unwrap();
    let recorded = Utc::now() - chrono::Duration::hours(7);
    runtime.rollout_store.append_usage_record(
        &path,
        source,
        devo_protocol::native::usage::UsageRecord {
            call_id: "prior-extraction".into(),
            session_id: devo_protocol::native::ids::SessionId::from_legacy_uuid(uuid::Uuid::from(
                source,
            )),
            turn_id: None,
            purpose: devo_protocol::native::usage::UsagePurpose::MemoryExtraction,
            model: devo_protocol::native::model::ModelBinding {
                provider: "test".into(),
                model: "test-fast".into(),
                variant: None,
                reasoning_effort: None,
            },
            outcome: devo_protocol::native::usage::UsageCallOutcome::Succeeded,
            usage: None,
            estimated_cost: None,
            recorded_at: recorded,
        },
    )?;
    let watermark = format!("{:x}", Sha256::digest(std::fs::read(path)?));
    let db = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
    db.execute("INSERT INTO memory_job_receipts(source_session_id, source_watermark, completed_at, job_kind)
        VALUES (?1, ?2, ?3, 'source_scan')", rusqlite::params![source.to_string(), watermark, recorded.to_rfc3339()])?;
    db.execute(
        "UPDATE memory_schema_meta SET value = '10' WHERE key = 'schema_version'",
        [],
    )?;
    drop(db);
    runtime.shutdown().await;
    drop(runtime);
    let runtime = open_scan_runtime(root.path(), Arc::clone(&provider))?;
    scan(&runtime, root.path()).await?;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    let connection = connect(&runtime).await?;
    let status = rpc(&runtime, connection, "memory/status", json!({})).await?;
    assert_eq!(status["result"].get("rebuild"), None);
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-3, DD-9. Changing Project identity during a reread revokes permission to send that source.
#[tokio::test]
async fn project_identity_change_during_rebuild_prevents_dispatch() -> Result<()> {
    use crate::memory::source_read_test_support::{ReadPoint, on_read};
    let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 1)?;
    let connection = connect(&runtime).await?;
    bind_source(&runtime, connection).await?;
    authorize(
        &runtime,
        connection,
        devo_protocol::native::rpc_memory::MemoryScope::Project,
    )
    .await?;
    let source = runtime.deps.db.list_root_sessions()?[0].session_id;
    let path = runtime
        .deps
        .db
        .get_session_index(&source)?
        .unwrap()
        .rollout_path
        .unwrap();
    let git = root.path().join(".git");
    let _hook = on_read(&path, ReadPoint::Complete, /*skip_reads*/ 1, move || {
        std::fs::create_dir(&git).unwrap();
    });
    scan(&runtime, root.path()).await?;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-9, DD-12.
/// Verifies: v10 receipts survive rebuild accounting before or after the trigger's own scan, restart, and semantic changes.
#[tokio::test]
async fn legacy_receipts_survive_rebuild_accounting_and_restart() -> Result<()> {
    use sha2::{Digest, Sha256};
    for source_count in [2, 1] {
        for receipt_sql in [
            "INSERT INTO memory_job_receipts(source_session_id, source_watermark, completed_at, job_kind)
             VALUES (?1, ?2, ?3, 'source_scan')",
            "INSERT INTO memory_jobs(job_id, job_key, source_session_id, source_watermark, job_kind,
                state, created_at, updated_at)
             VALUES (?1, ?1, ?1, ?2, 'source_scan', 'completed', ?3, ?3)",
        ] {
            let (root, runtime, provider) = setup(source_count, /*permits*/ 8)?;
            let sources = runtime.deps.db.list_sessions()?;
            let trigger = sources.last().unwrap().session_id;
            let recorded = Utc::now() - chrono::Duration::hours(7);
            let db = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
            for source in &sources {
                let path = runtime.deps.db.get_session_index(&source.session_id)?
                    .unwrap().rollout_path.unwrap();
                runtime.rollout_store.append_usage_record(
                    &path,
                    source.session_id,
                    devo_protocol::native::usage::UsageRecord {
                        call_id: "prior-extraction".into(),
                        session_id: devo_protocol::native::ids::SessionId::from_legacy_uuid(
                            uuid::Uuid::from(source.session_id),
                        ),
                        turn_id: None,
                        purpose: devo_protocol::native::usage::UsagePurpose::MemoryExtraction,
                        model: devo_protocol::native::model::ModelBinding {
                            provider: "test".into(), model: "test-fast".into(),
                            variant: None, reasoning_effort: None,
                        },
                        outcome: devo_protocol::native::usage::UsageCallOutcome::Succeeded,
                        usage: None, estimated_cost: None, recorded_at: recorded,
                    },
                )?;
                let watermark = format!("{:x}", Sha256::digest(std::fs::read(path)?));
                db.execute(receipt_sql, rusqlite::params![
                    source.session_id.to_string(), watermark, recorded.to_rfc3339()
                ])?;
            }
            db.execute("UPDATE memory_schema_meta SET value = '10' WHERE key = 'schema_version'", [])?;
            drop(db);
            runtime.shutdown().await;
            drop(runtime);
            let runtime = open_scan_runtime(root.path(), Arc::clone(&provider))?;
            let connection = connect(&runtime).await?;
            bind_source(&runtime, connection).await?;
            authorize(&runtime, connection, devo_protocol::native::rpc_memory::MemoryScope::User).await?;
            let path = runtime.deps.db.get_session_index(&trigger)?.unwrap().rollout_path.unwrap();
            let before = std::fs::read(&path)?;
            // A later-listed trigger receives the first source's accounting before its own claim.
            Arc::clone(runtime.memory.as_ref().unwrap())
                .run_background_scan(crate::memory::scan::ScanContext {
                    db: Arc::clone(&runtime.deps.db),
                    model_context: runtime.deps.context_for_workspace(root.path()).await?,
                    usage_ledger: runtime.usage_ledger.clone(), triggering_session: trigger,
                    activity: Arc::new(IdleSources),
                }).await?;
            assert_eq!(provider.calls.load(Ordering::SeqCst), source_count);
            assert_ne!(std::fs::read(&path)?, before, "rebuild must persist accounting to its trigger");
            runtime.shutdown().await;
            drop(runtime);
            let runtime = open_scan_runtime(root.path(), Arc::clone(&provider))?;
            scan(&runtime, root.path()).await?;
            assert_eq!(provider.calls.load(Ordering::SeqCst), source_count,
                "ordinary scanning must not replay v10 history after rebuild accounting");
            // A later semantic append must not reuse a receipt for an earlier prefix.
            let lines = std::fs::read_to_string(&path)?.lines()
                .map(serde_json::from_str::<Value>).collect::<Result<Vec<_>, _>>()?;
            let mut changed = lines[2].clone();
            changed["Item"]["item"]["input_items"][0]["UserMessage"]["text"] = json!("I prefer spaces");
            use std::io::Write;
            writeln!(std::fs::OpenOptions::new().append(true).open(&path)?, "{}", changed)?;
            scan(&runtime, root.path()).await?;
            assert_eq!(provider.calls.load(Ordering::SeqCst), source_count + 1,
                "changed semantic history must still be scanned");
        }
    }
    Ok(())
}
