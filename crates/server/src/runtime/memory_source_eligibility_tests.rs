use super::*;
use pretty_assertions::assert_eq;

/// Trace: L2-DES-MEM-001 DD-7.
/// Verifies: every excluded source is rejected without extraction and exposes only a bounded reason code.
#[tokio::test]
async fn excluded_source_types_never_call_extractor_and_report_safe_reasons() -> Result<()> {
    for (case, reason) in [
        ("external", "external_context_used"),
        ("subagent", "non_root"),
        ("ephemeral", "ephemeral"),
        ("automation", "automation"),
        ("native_ephemeral", "ephemeral"),
        ("legacy_automation", "automation"),
        ("fork", "fork_history"),
        ("missing_path", "not_persisted"),
    ] {
        let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 1)?;
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
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<Result<Vec<_>, _>>()?;
        match case {
            "external" => {
                runtime
                    .rollout_store
                    .mark_external_context_used_at(&path, metadata.session_id)?;
            }
            "subagent" => {
                metadata.parent_session_id = Some(SessionId::new());
                metadata.agent_path = Some("root/child".into());
                lines[0]["SessionMeta"]["session"]["parent_session_id"] =
                    serde_json::json!(metadata.parent_session_id);
            }
            "ephemeral" => {
                let connection = rusqlite::Connection::open(root.path().join("devo.db"))?;
                connection.execute(
                    "UPDATE sessions SET ephemeral = 1 WHERE id = ?1",
                    [metadata.session_id.to_string()],
                )?;
                metadata.ephemeral = true;
            }
            "automation" => {
                lines.push(serde_json::json!({"v":2,"kind":"internal","timestamp":Utc::now(),
                    "sessionId":metadata.session_id,"turnId":null,"seq":0,"entry":{
                    "type":"sessionSettings","schemaVersion":1,"field":"sessionSource","value":"automation","epoch":1}}));
            }
            "native_ephemeral" => {
                let mut projector = devo_core::LegacyProjector::new();
                lines = lines
                    .iter()
                    .flat_map(|line| {
                        let devo_core::ParsedRolloutLine::Legacy(line) =
                            devo_core::parse_rollout_line(&line.to_string()).unwrap()
                        else {
                            panic!("legacy fixture")
                        };
                        projector
                            .project_line(&line)
                            .unwrap()
                            .iter()
                            .map(serde_json::to_value)
                            .collect::<Result<Vec<_>, _>>()
                            .unwrap()
                    })
                    .collect();
                lines[0]["session"]["ephemeral"] = serde_json::json!(true);
            }
            "legacy_automation" => {
                lines[0]["SessionMeta"]["session"]["source"] = serde_json::json!("heartbeat");
            }
            "fork" => {
                metadata.fork_from_id = Some(SessionId::new());
                lines[0]["SessionMeta"]["session"]["fork_from_id"] =
                    serde_json::json!(metadata.fork_from_id);
            }
            "missing_path" => {
                let connection = rusqlite::Connection::open(root.path().join("devo.db"))?;
                connection.execute(
                    "UPDATE sessions SET rollout_path = NULL WHERE id = ?1",
                    [metadata.session_id.to_string()],
                )?;
            }
            _ => unreachable!("literal test cases"),
        }
        if case != "external" {
            std::fs::write(
                &path,
                lines
                    .iter()
                    .map(serde_json::to_string)
                    .collect::<Result<Vec<_>, _>>()?
                    .join("\n")
                    + "\n",
            )?;
        }
        if case != "missing_path" {
            runtime.deps.db.upsert_session(&metadata, Some(&path))?;
        }
        scan(&runtime, root.path()).await?;
        let result = runtime
            .memory
            .as_ref()
            .unwrap()
            .execute_command(crate::memory::MemoryCommand::Status)
            .await?;
        let crate::memory::MemoryCommandResult::Status(status) = result else {
            panic!("status result")
        };
        let status = serde_json::to_value(status)?;
        assert_eq!(
            status["sourceExclusionReasons"],
            serde_json::json!([reason]),
            "{case}"
        );
        assert_eq!(
            (
                provider.calls.load(Ordering::SeqCst),
                status["pendingJobCount"].clone(),
                status["entryCount"].clone()
            ),
            (0, serde_json::json!(0), serde_json::json!(0)),
            "{case}"
        );
        assert!(!status.to_string().contains("I prefer tabs"));
        assert!(
            !status
                .to_string()
                .contains(&metadata.session_id.to_string())
        );
    }
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-7/DD-5.
/// Verifies: excluding an external source preserves explicit remember and its credential validation.
#[tokio::test]
async fn excluded_external_source_can_remember_only_validated_explicit_content() -> Result<()> {
    let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 1)?;
    let metadata = runtime.deps.db.list_root_sessions()?[0].clone();
    let path = runtime
        .deps
        .db
        .get_session_index(&metadata.session_id)?
        .unwrap()
        .rollout_path
        .unwrap();
    runtime
        .rollout_store
        .mark_external_context_used_at(&path, metadata.session_id)?;
    scan(&runtime, root.path()).await?;
    let memory = runtime.memory.as_ref().unwrap();
    for (text, accepted) in [("Use tabs", true), ("API key: ab", false)] {
        let result = memory
            .execute_command(crate::memory::MemoryCommand::Remember(
                crate::memory::MemoryRememberRequest {
                    text: text.into(),
                    scope: devo_protocol::native::rpc_memory::MemoryScope::User,
                    kind: None,
                    source: crate::memory::MemorySourceContext {
                        user_item_id: Some(devo_protocol::native::ids::ItemId::new()),
                        session_id: metadata.session_id,
                        turn_id: Some(TurnId::new()),
                        workspace_root: root.path().to_path_buf(),
                    },
                },
            ))
            .await;
        match result {
            Ok(crate::memory::MemoryCommandResult::Remember(entry)) => {
                assert!(accepted);
                assert_eq!(entry.body, "Use tabs");
            }
            Err(crate::memory::MemoryError::SecretContentRejected) => assert!(!accepted),
            result => panic!("unexpected explicit remember result: {result:?}"),
        }
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: an exclusion arriving after the pre-attempt read prevents any extractor request.
#[tokio::test]
async fn external_fence_after_source_read_prevents_extraction() -> Result<()> {
    use crate::memory::source_read_test_support::{ReadPoint, on_read};

    let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 1)?;
    let metadata = runtime.deps.db.list_root_sessions()?[0].clone();
    let path = runtime
        .deps
        .db
        .get_session_index(&metadata.session_id)?
        .unwrap()
        .rollout_path
        .unwrap();
    let memory = Arc::clone(runtime.memory.as_ref().unwrap());
    let store = runtime.rollout_store.clone();
    let marker_path = path.clone();
    // Skip admission's first read; mark the completed pre-attempt snapshot.
    let _hook = on_read(&path, ReadPoint::Complete, /*skip_reads*/ 1, move || {
        memory.begin_external_context_sources(&[metadata.session_id]);
        store
            .mark_external_context_used_at(&marker_path, metadata.session_id)
            .unwrap();
    });
    scan(&runtime, root.path()).await?;
    let entries = runtime
        .memory
        .as_ref()
        .unwrap()
        .execute_command(crate::memory::MemoryCommand::List(
            crate::memory::ListMemoryRequest::default(),
        ))
        .await?;
    let crate::memory::MemoryCommandResult::List(entries) = entries else {
        panic!("list result")
    };
    let connection = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
    let job = connection.query_row(
        "SELECT state, attempt_count, lease_owner, lease_until FROM memory_jobs
         WHERE source_session_id = ?1",
        [metadata.session_id.to_string()],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, u32>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        },
    )?;
    assert_eq!(
        (provider.calls.load(Ordering::SeqCst), entries.data, job),
        (0, Vec::new(), ("pending".into(), 0, None, None))
    );
    Ok(())
}
