use std::sync::Arc;

use anyhow::{Context, Result};
use devo_server::ServerRuntime;
use pretty_assertions::assert_eq;
use serde_json::{Value, json};

#[path = "support/subagent_lifecycle.rs"]
#[allow(dead_code)]
mod support;

fn runtime(root: &std::path::Path) -> Result<Arc<ServerRuntime>> {
    support::build_runtime_with_workspace_config(root, Arc::new(support::ScriptedProvider::new([])))
}

async fn rpc(runtime: &Arc<ServerRuntime>, connection: u64, method: &str, params: Value) -> Value {
    runtime
        .handle_incoming(
            connection,
            json!({"id": 100, "method": method, "params": params}),
        )
        .await
        .expect("Native response")
}

async fn bound_connection(
    runtime: &Arc<ServerRuntime>,
    workspace: &std::path::Path,
) -> Result<u64> {
    let (connection, _notifications) = support::initialize_connection(runtime).await?;
    let session = support::start_parent_session(runtime, connection, workspace).await?;
    let response = rpc(
        runtime,
        connection,
        "subscription/create",
        json!({
            "selectors": [{"kind": "session", "sessionId": session}], "includeSnapshot": false
        }),
    )
    .await;
    anyhow::ensure!(
        response.get("result").is_some(),
        "subscription failed: {response}"
    );
    Ok(connection)
}

fn configured_root() -> Result<tempfile::TempDir> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join(".devo"))?;
    std::fs::write(
        root.path().join(".devo/config.toml"),
        "[memory]\nenabled = true\n",
    )?;
    Ok(root)
}

/// Trace: L2-DES-MEM-001 DD-9, DD-10. No implicit scope can clear user memory.
#[tokio::test]
async fn native_export_and_reset_require_explicit_scope() -> Result<()> {
    let root = configured_root()?;
    let runtime = runtime(root.path())?;
    let (connection, _notifications) = support::initialize_connection(&runtime).await?;
    for method in ["memory/export", "memory/reset"] {
        for params in [json!({}), json!({"scope": "all"}), json!({"scope": null})] {
            let response = rpc(&runtime, connection, method, params).await;
            assert_eq!(
                response["error"]["code"].clone(),
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
        let response = rpc(&runtime, connection, method, json!({"scope": "project"})).await;
        assert_eq!(
            response["error"]["code"].clone(),
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

/// Trace: L2-DES-MEM-001 DD-3, DD-9, DD-12. Native reset is scoped and durable.
#[tokio::test]
async fn native_export_reset_scope_isolation_survives_restart() -> Result<()> {
    let root = configured_root()?;
    let project = root.path().join("project");
    let other = root.path().join("other");
    std::fs::create_dir(&project)?;
    std::fs::create_dir(&other)?;
    let server = runtime(root.path())?;
    let connection = bound_connection(&server, &project).await?;
    let other_connection = bound_connection(&server, &other).await?;
    for (client, scope, text) in [
        (connection, "user", "User knowledge"),
        (connection, "project", "Project knowledge"),
        (other_connection, "project", "Other knowledge"),
    ] {
        let response = rpc(
            &server,
            client,
            "memory/remember",
            json!({"scope": scope, "text": text}),
        )
        .await;
        anyhow::ensure!(
            response.get("result").is_some(),
            "remember failed: {response}"
        );
    }
    for scope in ["user", "project"] {
        let exported = rpc(
            &server,
            connection,
            "memory/export",
            json!({"scope": scope}),
        )
        .await;
        let markdown = exported["result"]["markdown"]
            .as_str()
            .context(format!("export failed: {exported}"))?;
        assert!(markdown.contains(if scope == "user" {
            "User knowledge"
        } else {
            "Project knowledge"
        }));
        assert!(!markdown.contains("Other knowledge"));
        assert_eq!(
            exported["result"]["lifecycle"]["ignoreSourcesBefore"],
            Value::Null
        );
    }
    let reset = rpc(
        &server,
        connection,
        "memory/reset",
        json!({"scope": "project"}),
    )
    .await;
    assert_eq!(reset["result"]["clearedEntryCount"], json!(1), "{reset}");
    assert_eq!(reset["result"]["clearedCandidateCount"], json!(0));
    let watermark = reset["result"]["ignoreSourcesBefore"].clone();
    assert!(watermark.as_str().is_some());
    let empty = rpc(
        &server,
        connection,
        "memory/list",
        json!({"scope": "project"}),
    )
    .await;
    assert_eq!(empty["result"], json!({"data": []}));
    let status = rpc(&server, connection, "memory/status", json!({})).await;
    assert_eq!(status["result"]["entryCount"], json!(2));
    drop(server);
    let server = runtime(root.path())?;
    let connection = bound_connection(&server, &project).await?;
    let other_connection = bound_connection(&server, &other).await?;
    let exported = rpc(
        &server,
        connection,
        "memory/export",
        json!({"scope": "project"}),
    )
    .await;
    let markdown = exported["result"]["markdown"]
        .as_str()
        .context("restart export")?;
    assert!(!markdown.contains("Project knowledge"));
    assert_eq!(
        exported["result"]["lifecycle"]["ignoreSourcesBefore"],
        watermark
    );
    let user = rpc(&server, connection, "memory/list", json!({"scope": "user"})).await;
    let other = rpc(
        &server,
        other_connection,
        "memory/list",
        json!({"scope": "project"}),
    )
    .await;
    assert_eq!(user["result"]["data"].as_array().unwrap().len(), 1);
    assert_eq!(other["result"]["data"].as_array().unwrap().len(), 1);
    let reset = rpc(
        &server,
        connection,
        "memory/reset",
        json!({"scope": "user"}),
    )
    .await;
    assert_eq!(reset["result"]["clearedEntryCount"], json!(1), "{reset}");
    let status = rpc(&server, connection, "memory/status", json!({})).await;
    assert_eq!(status["result"]["entryCount"], json!(1));
    let other = rpc(
        &server,
        other_connection,
        "memory/list",
        json!({"scope": "project"}),
    )
    .await;
    assert_eq!(other["result"]["data"].as_array().unwrap().len(), 1);
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-6, DD-9, DD-12. Export reads all safe lifecycle records.
#[tokio::test]
async fn native_export_is_unpaginated_redacted_and_reset_clears_candidates() -> Result<()> {
    let root = configured_root()?;
    let server = runtime(root.path())?;
    let connection = bound_connection(&server, root.path()).await?;
    let response = rpc(
        &server,
        connection,
        "memory/remember",
        json!({"scope": "user", "text": "seed"}),
    )
    .await;
    anyhow::ensure!(
        response.get("result").is_some(),
        "remember failed: {response}"
    );
    let db = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
    for (index, state) in ["active", "stale", "conflicted", "retired", "restored"]
        .into_iter()
        .enumerate()
    {
        db.execute(
            "INSERT INTO memory_entries(entry_id, scope_type, scope_id, kind, normalized_key,
            body, origin, state, created_at, updated_at) VALUES (?1, 'user', 'user', 'fact', ?1, ?2,
            'explicit_user', ?3, '2026-10-08T00:00:00Z', '2026-10-08T00:00:00Z')",
            rusqlite::params![
                format!("lifecycle-{index}"),
                format!("visible {state}"),
                state
            ],
        )?;
    }
    for index in 0..105 {
        db.execute(
            "INSERT INTO memory_entries(entry_id, scope_type, scope_id, kind, normalized_key,
            body, origin, state, created_at, updated_at) VALUES (?1, 'user', 'user', 'fact', ?1, ?1,
            'explicit_user', 'active', '2026-10-08T00:00:00Z', '2026-10-08T00:00:00Z')",
            [format!("export-marker-{index:03}")],
        )?;
    }
    db.execute("INSERT INTO memory_entries(entry_id, scope_type, scope_id, kind, normalized_key,
        body, origin, state, created_at, updated_at) VALUES ('unsafe', 'user', 'user', 'fact', 'unsafe',
        'api_key=private-value', 'explicit_user', 'retired', '2026-10-08T00:00:00Z', '2026-10-08T00:00:00Z')", [])?;
    db.execute(
        "INSERT INTO memory_candidates(candidate_id, scope_type, scope_id, kind,
        normalized_key, body, origin, source_session_id, retention_until, created_at)
        VALUES ('candidate', 'user', 'user', 'fact', 'candidate', 'RAW EXTRACTION SENTINEL',
        'inferred_session', 'source', '2030-01-01T00:00:00Z', '2026-10-08T00:00:00Z')",
        [],
    )?;
    db.execute("INSERT INTO memory_revocations(revocation_id, scope_type, scope_id, normalized_key, revoked_at)
        VALUES ('revocation', 'user', 'user', 'secret historical key', '2026-10-08T00:00:00Z')", [])?;
    std::fs::write(
        root.path().join("memory/user/MEMORY.md"),
        "RAW TRANSCRIPT SENTINEL",
    )?;
    let exported = rpc(
        &server,
        connection,
        "memory/export",
        json!({"scope": "user"}),
    )
    .await;
    let markdown = exported["result"]["markdown"]
        .as_str()
        .context(format!("export failed: {exported}"))?;
    for state in ["active", "stale", "conflicted", "retired", "restored"] {
        assert!(markdown.contains(&format!("visible {state}")));
        assert!(markdown.contains(&format!("state: {state}")));
    }
    for index in 0..105 {
        assert!(markdown.contains(&format!("export-marker-{index:03}")));
    }
    for forbidden in [
        "private-value",
        "RAW EXTRACTION SENTINEL",
        "RAW TRANSCRIPT SENTINEL",
        "secret historical key",
    ] {
        assert!(!exported.to_string().contains(forbidden));
    }
    assert_eq!(
        exported["result"]["lifecycle"],
        json!({
            "ignoreSourcesBefore": null, "lastRebuildAt": null, "revocationCount": 1
        })
    );
    let reset = rpc(
        &server,
        connection,
        "memory/reset",
        json!({"scope": "user"}),
    )
    .await;
    assert_eq!(
        (
            reset["result"]["clearedEntryCount"].clone(),
            reset["result"]["clearedCandidateCount"].clone()
        ),
        (json!(112), json!(1))
    );
    let status = rpc(&server, connection, "memory/status", json!({})).await;
    assert_eq!(
        (
            status["result"]["entryCount"].clone(),
            status["result"]["candidateCount"].clone()
        ),
        (json!(0), json!(0))
    );
    let counts: (u64, u64, u64) = db.query_row(
        "SELECT
        (SELECT COUNT(*) FROM memory_entries_fts), (SELECT COUNT(*) FROM memory_evidence),
        (SELECT COUNT(*) FROM memory_revocations)",
        [],
        |row| {
            Ok((
                row.get(/*idx*/ 0)?,
                row.get(/*idx*/ 1)?,
                row.get(/*idx*/ 2)?,
            ))
        },
    )?;
    assert_eq!(counts, (0, 0, 0));
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-2. Disabled memory rejects export and reset before selectors.
#[tokio::test]
async fn native_export_reset_respect_global_gate() -> Result<()> {
    let root = tempfile::tempdir()?;
    let server = runtime(root.path())?;
    let (connection, _notifications) = support::initialize_connection(&server).await?;
    for method in ["memory/export", "memory/reset"] {
        for scope in ["user", "project"] {
            let response = rpc(&server, connection, method, json!({"scope": scope})).await;
            assert_eq!(
                response["error"]["code"],
                json!("InternalError"),
                "{response}"
            );
            assert_eq!(response["error"]["message"], json!("memory is disabled"));
        }
    }
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-3. Conflicting Project selectors cannot choose a reset target.
#[tokio::test]
async fn native_export_reset_reject_ambiguous_projects_without_mutation() -> Result<()> {
    let root = configured_root()?;
    let project = root.path().join("project");
    let other = root.path().join("other");
    std::fs::create_dir(&project)?;
    std::fs::create_dir(&other)?;
    let server = runtime(root.path())?;
    let (connection, _notifications) = support::initialize_connection(&server).await?;
    let first = support::start_parent_session(&server, connection, &project).await?;
    let second = support::start_parent_session(&server, connection, &other).await?;
    let response = rpc(&server, connection, "subscription/create", json!({
        "selectors": [{"kind": "session", "sessionId": first}, {"kind": "session", "sessionId": second}],
        "includeSnapshot": false
    })).await;
    anyhow::ensure!(
        response.get("result").is_some(),
        "subscription failed: {response}"
    );
    for method in ["memory/export", "memory/reset"] {
        let response = rpc(&server, connection, method, json!({"scope": "project"})).await;
        assert_eq!(
            response["error"]["code"],
            json!("InvalidParams"),
            "{response}"
        );
        assert!(
            response["error"]["message"]
                .as_str()
                .unwrap()
                .contains("ambiguous")
        );
    }
    let db = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
    let fences: u64 = db.query_row(
        "SELECT COUNT(*) FROM memory_scope_state WHERE ignore_sources_before IS NOT NULL",
        [],
        |row| row.get(/*idx*/ 0),
    )?;
    assert_eq!(fences, 0);
    Ok(())
}

/// Trace: L2-DES-MEM-001 DD-6, DD-9. User reset needs an unambiguous interactive command caller.
#[tokio::test]
async fn native_user_reset_rejects_unbound_ambiguous_and_automation_callers() -> Result<()> {
    let root = configured_root()?;
    let server = runtime(root.path())?;
    let (connection, _notifications) = support::initialize_connection(&server).await?;
    let unbound = rpc(
        &server,
        connection,
        "memory/reset",
        json!({"scope": "user"}),
    )
    .await;
    assert_eq!(
        unbound["error"]["code"],
        json!("InvalidParams"),
        "{unbound}"
    );
    let first = support::start_parent_session(&server, connection, root.path()).await?;
    let second = support::start_parent_session(&server, connection, root.path()).await?;
    let response = rpc(&server, connection, "subscription/create", json!({
        "selectors": [{"kind": "session", "sessionId": first}, {"kind": "session", "sessionId": second}],
        "includeSnapshot": false
    })).await;
    anyhow::ensure!(
        response.get("result").is_some(),
        "subscription failed: {response}"
    );
    let ambiguous = rpc(
        &server,
        connection,
        "memory/reset",
        json!({"scope": "user"}),
    )
    .await;
    assert_eq!(
        ambiguous["error"]["code"],
        json!("InvalidParams"),
        "{ambiguous}"
    );
    let (automation_connection, _notifications) = support::initialize_connection(&server).await?;
    let automation = rpc(
        &server,
        automation_connection,
        "session/new",
        json!({
            "cwd": root.path(), "idempotencyKey": "reset-automation", "source": "automation"
        }),
    )
    .await;
    let id = automation["result"]["session"]["id"]
        .as_str()
        .context(format!("automation session: {automation}"))?;
    let response = rpc(
        &server,
        automation_connection,
        "subscription/create",
        json!({
            "selectors": [{"kind": "session", "sessionId": id}], "includeSnapshot": false
        }),
    )
    .await;
    anyhow::ensure!(
        response.get("result").is_some(),
        "subscription failed: {response}"
    );
    let reset = rpc(
        &server,
        automation_connection,
        "memory/reset",
        json!({"scope": "user"}),
    )
    .await;
    assert_eq!(reset["error"]["code"], json!("InvalidParams"), "{reset}");
    let db = rusqlite::Connection::open(root.path().join("memory/memory.sqlite3"))?;
    let fences: u64 = db.query_row(
        "SELECT COUNT(*) FROM memory_scope_state WHERE ignore_sources_before IS NOT NULL",
        [],
        |row| row.get(/*idx*/ 0),
    )?;
    assert_eq!(fences, 0);
    Ok(())
}
