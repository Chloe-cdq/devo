use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use devo_protocol::native::ids::{ItemId, SessionId, TurnId};
use devo_protocol::native::rpc_memory::{MemoryKind, MemoryResetResult, MemoryScope};
use devo_protocol::native::session::MemorySetting;
use pretty_assertions::assert_eq;

use super::extraction::ExtractionCandidate;
use super::runtime_test_support::remember_request;
use super::source::{ExtractableSource, SourceMessage};
use super::{MemoryCommand, MemoryCommandResult, MemoryError, MemoryRuntime, ScopedMemoryRequest};

fn epoch() -> DateTime<Utc> {
    "2030-01-01T00:00:00Z".parse().unwrap()
}

fn open(root: &Path) -> MemoryRuntime {
    MemoryRuntime::open_with_clock(
        root.to_path_buf(),
        devo_core::MemoryConfig {
            enabled: true,
            min_source_idle_hours: 0,
            ..Default::default()
        },
        Arc::new(epoch),
    )
    .unwrap()
}

fn request(scope: MemoryScope, workspace: &Path) -> ScopedMemoryRequest {
    ScopedMemoryRequest {
        scope,
        workspace_root: workspace.to_path_buf(),
    }
}

/// Trace: L2-DES-MEM-001 DD-9. Reset and source watermark are a single transaction.
#[tokio::test]
async fn reset_rollback_preserves_entries_index_and_watermark() {
    let root = tempfile::tempdir().unwrap();
    let memory = open(root.path());
    memory
        .execute_command(MemoryCommand::Remember(remember_request("Use tabs")))
        .await
        .unwrap();
    seed_scope_artifacts(&memory);
    let projection = std::fs::read(root.path().join("user/MEMORY.md")).unwrap();
    {
        let db = memory.connection.lock().unwrap();
        db.execute_batch(
            "CREATE TRIGGER prevent_reset BEFORE DELETE ON memory_entries
            BEGIN SELECT RAISE(ABORT, 'injected reset failure'); END;",
        )
        .unwrap();
    }
    let result = memory
        .execute_command(MemoryCommand::Reset(request(
            MemoryScope::User,
            Path::new(""),
        )))
        .await;
    assert!(matches!(result, Err(MemoryError::Database(_))));
    assert_eq!(
        std::fs::read(root.path().join("user/MEMORY.md")).unwrap(),
        projection
    );
    let db = memory.connection.lock().unwrap();
    let counts: (u64, u64, u64) = db
        .query_row(
            "SELECT
        (SELECT COUNT(*) FROM memory_entries), (SELECT COUNT(*) FROM memory_entries_fts),
        (SELECT COUNT(*) FROM memory_scope_state WHERE ignore_sources_before IS NOT NULL)",
            [],
            |row| {
                Ok((
                    row.get(/*idx*/ 0)?,
                    row.get(/*idx*/ 1)?,
                    row.get(/*idx*/ 2)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(counts, (1, 1, 0));
    let artifact_counts: (u64, u64, u64, u64, u64) = db.query_row("SELECT
        (SELECT COUNT(*) FROM memory_candidates), (SELECT COUNT(*) FROM memory_proposal_claims),
        (SELECT COUNT(*) FROM memory_proposal_claim_sources), (SELECT COUNT(*) FROM memory_revocations),
        (SELECT COUNT(*) FROM memory_evidence)", [],
        |row| Ok((row.get(/*idx*/ 0)?, row.get(/*idx*/ 1)?, row.get(/*idx*/ 2)?, row.get(/*idx*/ 3)?, row.get(/*idx*/ 4)?))).unwrap();
    assert_eq!(artifact_counts, (1, 1, 1, 1, 1));
}

/// Trace: L2-DES-MEM-001 DD-9, DD-12. In-flight old sources stay fenced after restart.
#[tokio::test]
async fn reset_fences_old_evidence_but_keeps_other_scopes_and_new_sources() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let memory = open(root.path());
    let turn = TurnId::new();
    let mut source = ExtractableSource {
        session_id: SessionId::new(),
        workspace_root: workspace.clone(),
        session_contribution: MemorySetting::On,
        observed_at: epoch() - Duration::hours(1),
        watermark: "old-snapshot".into(),
        legacy_watermark: None,
        messages: vec![SourceMessage {
            turn_id: turn.clone(),
            item_id: ItemId::new(),
            role: "user".into(),
            observed_at: epoch() - Duration::hours(1),
            text: "Use tabs".into(),
        }],
    };
    let candidate = ExtractionCandidate {
        scope: MemoryScope::User,
        kind: MemoryKind::Preference,
        key: "indentation".into(),
        body: "Use tabs".into(),
        evidence: vec![turn],
    };
    let candidates = [
        candidate.clone(),
        ExtractionCandidate {
            scope: MemoryScope::Project,
            ..candidate.clone()
        },
    ];
    let claim = memory.claim_source(&source, epoch()).unwrap().unwrap();
    memory
        .execute_command(MemoryCommand::Reset(request(MemoryScope::User, &workspace)))
        .await
        .unwrap();
    drop(memory);
    let memory = open(root.path());
    // The journal's latest timestamp is newer, but the actual cited evidence is old.
    source.observed_at = epoch() + Duration::seconds(1);
    memory
        .commit_extraction(&claim, &source, &candidates, epoch() + Duration::seconds(2))
        .unwrap();
    assert!(
        memory
            .list(super::ListMemoryRequest::default())
            .unwrap()
            .data
            .is_empty()
    );
    assert_eq!(
        memory
            .list(super::ListMemoryRequest {
                scope: Some(MemoryScope::Project),
                workspace_root: workspace.clone(),
                ..Default::default()
            })
            .unwrap()
            .data
            .len(),
        1
    );
    {
        let db = memory.connection.lock().unwrap();
        let counts: (u64, u64) = db
            .query_row(
                "SELECT
            (SELECT COUNT(*) FROM memory_candidates WHERE scope_type = 'user'),
            (SELECT COUNT(*) FROM memory_candidates WHERE scope_type = 'project')",
                [],
                |row| Ok((row.get(/*idx*/ 0)?, row.get(/*idx*/ 1)?)),
            )
            .unwrap();
        assert_eq!(counts, (0, 1));
    }
    source.session_id = SessionId::new();
    source.watermark = "boundary".into();
    source.messages[0].observed_at = epoch();
    source.observed_at = epoch();
    let claim = memory
        .claim_source(&source, epoch() + Duration::seconds(3))
        .unwrap()
        .unwrap();
    memory
        .commit_extraction(
            &claim,
            &source,
            std::slice::from_ref(&candidate),
            epoch() + Duration::seconds(3),
        )
        .unwrap();
    assert!(
        memory
            .list(super::ListMemoryRequest::default())
            .unwrap()
            .data
            .is_empty()
    );
    source.session_id = SessionId::new();
    source.watermark = "new-session".into();
    source.messages[0].observed_at = epoch() + Duration::seconds(1);
    source.observed_at = source.messages[0].observed_at;
    let claim = memory
        .claim_source(&source, epoch() + Duration::seconds(4))
        .unwrap()
        .unwrap();
    memory
        .commit_extraction(
            &claim,
            &source,
            &[candidate],
            epoch() + Duration::seconds(4),
        )
        .unwrap();
    assert_eq!(
        memory
            .list(super::ListMemoryRequest::default())
            .unwrap()
            .data
            .len(),
        1
    );
    let status = memory.status().unwrap();
    assert_eq!((status.entry_count, status.candidate_count), (2, 2));
}

/// Trace: L2-DES-MEM-001 DD-9. A committed reset remains authoritative after projection failure.
#[tokio::test]
async fn reset_projection_failure_is_repaired_on_restart() {
    let root = tempfile::tempdir().unwrap();
    let memory = open(root.path());
    memory
        .execute_command(MemoryCommand::Remember(remember_request("Use tabs")))
        .await
        .unwrap();
    let path = root.path().join("user/MEMORY.md");
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    let result = memory
        .execute_command(MemoryCommand::Reset(request(
            MemoryScope::User,
            Path::new(""),
        )))
        .await;
    let Err(MemoryError::ResetCommitted { result, .. }) = result else {
        panic!("committed reset error");
    };
    assert_eq!(
        *result,
        MemoryResetResult {
            scope: MemoryScope::User,
            cleared_entry_count: 1,
            cleared_candidate_count: 0,
            ignore_sources_before: epoch(),
        }
    );
    drop(memory);
    std::fs::remove_dir(&path).unwrap();
    let memory = open(root.path());
    let MemoryCommandResult::Export(export) = memory
        .execute_command(MemoryCommand::Export(request(
            MemoryScope::User,
            Path::new(""),
        )))
        .await
        .unwrap()
    else {
        panic!("export result");
    };
    assert_eq!(std::fs::read_to_string(path).unwrap(), export.markdown);
    assert_eq!(export.lifecycle.ignore_sources_before, Some(epoch()));
    assert!(!export.markdown.contains("Use tabs"));
}

/// Trace: L2-DES-MEM-001 DD-9. Time correction cannot lower an existing exclusion fence.
#[tokio::test]
async fn repeated_reset_never_lowers_watermark() {
    let root = tempfile::tempdir().unwrap();
    let memory = open(root.path());
    let future = epoch() + Duration::hours(1);
    {
        let db = memory.connection.lock().unwrap();
        db.execute(
            "INSERT INTO memory_scope_state(scope_type, scope_id, ignore_sources_before)
            VALUES ('user', 'user', ?1)",
            [future.to_rfc3339()],
        )
        .unwrap();
    }
    let MemoryCommandResult::Reset(result) = memory
        .execute_command(MemoryCommand::Reset(request(
            MemoryScope::User,
            Path::new(""),
        )))
        .await
        .unwrap()
    else {
        panic!("reset result");
    };
    assert_eq!(
        result,
        MemoryResetResult {
            scope: MemoryScope::User,
            cleared_entry_count: 0,
            cleared_candidate_count: 0,
            ignore_sources_before: future,
        }
    );
}

/// Trace: L2-DES-MEM-001 Privacy and Authority. Pending source cleanup fences exports.
#[tokio::test]
async fn export_is_unavailable_while_source_cleanup_is_pending() {
    let root = tempfile::tempdir().unwrap();
    let memory = open(root.path());
    memory
        .execute_command(MemoryCommand::Remember(remember_request("Use tabs")))
        .await
        .unwrap();
    memory
        .pending_external_sources
        .lock()
        .unwrap()
        .insert(devo_protocol::SessionId::new());
    let result = memory
        .execute_command(MemoryCommand::Export(request(
            MemoryScope::User,
            Path::new(""),
        )))
        .await;
    assert!(matches!(result, Err(MemoryError::StorageBusy)));
}

/// Trace: L2-DES-MEM-001 Rev 4 Privacy and Authority.
/// Clearing the ledger after a snapshot cannot authorize withdrawn provenance.
#[tokio::test]
async fn export_rejects_pending_cleanup_even_when_ledger_clears_before_return() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(crate::db::Database::open(root.path().join("devo.db")).unwrap());
    let mut memory = open(&root.path().join("memory"));
    memory.attach_deletion_ledger(Arc::clone(&db));
    let remembered = remember_request("Use tabs");
    let source_id = remembered.source.session_id;
    memory
        .execute_command(MemoryCommand::Remember(remembered))
        .await
        .unwrap();
    let before = memory
        .export(request(MemoryScope::User, Path::new("")))
        .unwrap();
    assert!(before.markdown.contains(&source_id.to_string()));
    db.record_memory_source_deletions(&[source_id]).unwrap();

    // Run the snapshot phase, then let cleanup finish before the command's
    // final pending-intent check. Both phases use the real storage paths.
    let snapshot = memory.export(request(MemoryScope::User, Path::new("")));
    memory.reconcile_source_intents();
    assert!(!memory.has_pending_source_deletions());
    let after = memory
        .export(request(MemoryScope::User, Path::new("")))
        .unwrap();
    assert!(after.markdown.contains("Use tabs"));
    assert!(!after.markdown.contains(&source_id.to_string()));
    assert!(matches!(snapshot, Err(MemoryError::StorageBusy)));
}

/// Trace: L2-DES-MEM-001 DD-3, DD-9. Reset removes all selected artifacts and preserves their other-scope peers.
#[tokio::test]
async fn reset_removes_selected_artifacts_and_preserves_other_scope_artifacts() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let memory = open(root.path());
    for scope in [MemoryScope::User, MemoryScope::Project] {
        let mut remembered = remember_request("Use tabs");
        remembered.scope = scope;
        remembered.source.workspace_root = workspace.clone();
        memory
            .execute_command(MemoryCommand::Remember(remembered))
            .await
            .unwrap();
    }
    seed_scope_artifacts(&memory);
    let MemoryCommandResult::Reset(result) = memory
        .execute_command(MemoryCommand::Reset(request(MemoryScope::User, &workspace)))
        .await
        .unwrap()
    else {
        panic!("reset result");
    };
    assert_eq!(
        result,
        MemoryResetResult {
            scope: MemoryScope::User,
            cleared_entry_count: 1,
            cleared_candidate_count: 1,
            ignore_sources_before: epoch(),
        }
    );
    drop(memory);
    let memory = open(root.path());
    let db = memory.connection.lock().unwrap();
    for table in [
        "memory_entries",
        "memory_candidates",
        "memory_proposal_claims",
        "memory_proposal_claim_sources",
    ] {
        let rows = db
            .prepare(&format!(
                "SELECT scope_type, COUNT(*) FROM {table} GROUP BY scope_type"
            ))
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(/*idx*/ 0)?,
                    row.get::<_, u64>(/*idx*/ 1)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows, vec![("project".into(), 1)], "{table}");
    }
    let revocations = db.prepare("SELECT scope_type, COUNT(*) FROM memory_revocations GROUP BY scope_type ORDER BY scope_type").unwrap()
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))).unwrap()
        .collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(revocations, vec![("project".into(), 1), ("user".into(), 1)]);
    let index_and_evidence: (u64, u64) = db
        .query_row(
            "SELECT
        (SELECT COUNT(*) FROM memory_entries_fts), (SELECT COUNT(*) FROM memory_evidence)",
            [],
            |row| Ok((row.get(/*idx*/ 0)?, row.get(/*idx*/ 1)?)),
        )
        .unwrap();
    assert_eq!(index_and_evidence, (1, 1));
}

fn seed_scope_artifacts(memory: &MemoryRuntime) {
    let db = memory.connection.lock().unwrap();
    db.execute_batch("INSERT INTO memory_candidates(candidate_id, scope_type, scope_id, kind,
            normalized_key, body, origin, source_session_id, retention_until, created_at)
            SELECT entry_id, scope_type, scope_id, kind, normalized_key, body, 'inferred_session',
                'source', '2030-02-01T00:00:00Z', created_at FROM memory_entries;
            INSERT INTO memory_proposal_claims(scope_type, scope_id, proposal_key, canonical_key, entry_id)
            SELECT scope_type, scope_id, 'indentation', normalized_key, entry_id FROM memory_entries;
            INSERT INTO memory_proposal_claim_sources(scope_type, scope_id, proposal_key, canonical_key, source_session_id)
            SELECT scope_type, scope_id, 'indentation', normalized_key, 'source' FROM memory_entries;
            INSERT INTO memory_revocations(revocation_id, scope_type, scope_id, normalized_key, revoked_at)
            SELECT entry_id, scope_type, scope_id, normalized_key, '2029-01-01T00:00:00Z' FROM memory_entries;").unwrap();
}
