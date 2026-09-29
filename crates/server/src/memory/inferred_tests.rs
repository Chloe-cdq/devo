use crate::memory::extraction::ExtractionCandidate;
use crate::memory::runtime_test_support::{
    forget_request, open_runtime, prepare_forget, remember_request,
};
use crate::memory::source::{ExtractableSource, SourceMessage};
use crate::memory::{ListMemoryRequest, MemoryCommand, MemoryCommandResult, MemoryForgetSelector};
use chrono::{Duration, Utc};
use devo_protocol::native::ids::ItemId;
use devo_protocol::native::ids::{SessionId, TurnId};
use devo_protocol::native::rpc_memory::{
    MemoryEntry, MemoryKind, MemoryOrigin, MemoryProvenance, MemoryScope, MemoryState,
};
use devo_protocol::native::session::MemorySetting;
use pretty_assertions::assert_eq;

fn fixture() -> (ExtractableSource, ExtractionCandidate) {
    let turn_id = TurnId::new();
    let source = ExtractableSource {
        session_id: SessionId::new(),
        workspace_root: std::path::PathBuf::new(),
        session_contribution: MemorySetting::On,
        observed_at: Utc::now() - Duration::hours(7),
        watermark: "source-1".into(),
        messages: vec![SourceMessage {
            turn_id: turn_id.clone(),
            item_id: ItemId::new(),
            role: "user".into(),
            observed_at: Utc::now() - Duration::hours(7),
            text: "I prefer tabs".into(),
        }],
    };
    let candidate = ExtractionCandidate {
        scope: MemoryScope::User,
        kind: MemoryKind::Preference,
        key: "indentation preference".into(),
        body: "I prefer tabs".into(),
        evidence: vec![turn_id],
    };
    (source, candidate)
}

/// Trace: L2-DES-MEM-001 DD-6/DD-8
/// Verifies: a claimed extraction commits inferred memory, evidence, lexical index and job completion once.
#[test]
fn extraction_commit_indexes_and_finishes_watermark() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let (source, candidate) = fixture();
    let now = Utc::now();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &source, &[candidate.clone(), candidate], now)
        .unwrap();
    let entries = runtime.list(ListMemoryRequest::default()).unwrap().data;
    let timestamp = chrono::DateTime::from_timestamp_millis(now.timestamp_millis()).unwrap();
    assert_eq!(
        entries,
        vec![MemoryEntry {
            entry_id: entries[0].entry_id.clone(),
            scope: MemoryScope::User,
            scope_id: "user".into(),
            kind: MemoryKind::Preference,
            normalized_key: "i prefer tabs".into(),
            body: "I prefer tabs".into(),
            origin: MemoryOrigin::InferredSession,
            state: MemoryState::Active,
            created_at: timestamp,
            updated_at: timestamp,
            replacement_entry_id: None,
            provenance: vec![MemoryProvenance {
                source_session_id: Some(source.session_id.to_string()),
                source_turn_id: Some(source.messages[0].turn_id.to_string()),
                source_user_item_id: Some(source.messages[0].item_id.clone()),
            }],
        }]
    );
    let search = runtime
        .search(crate::memory::SearchMemoryRequest {
            query: "tabs".into(),
            scope: MemoryScope::User,
            kind: None,
            state: None,
            workspace_root: std::path::PathBuf::new(),
        })
        .unwrap();
    assert_eq!(search.data.len(), 1);
    assert!(
        std::fs::read_to_string(root.path().join("user/MEMORY.md"))
            .unwrap()
            .contains("I prefer tabs")
    );
    assert_eq!(
        runtime
            .claim_source(&source, now + Duration::hours(1))
            .unwrap(),
        None
    );
}

/// Trace: L2-DES-MEM-001 DD-8
/// Verifies: incompatible inferred claims retain conflict evidence and are excluded from lexical recall.
#[tokio::test]
async fn incompatible_inferred_claims_are_conflicted() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let (mut source, mut candidate) = fixture();
    let now = Utc::now();
    let first = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime
        .commit_extraction(&first, &source, &[candidate.clone()], now)
        .unwrap();
    let supported = runtime
        .list(ListMemoryRequest::default())
        .unwrap()
        .data
        .remove(0);
    let first_source_id = source.session_id.to_string();
    source.session_id = SessionId::new();
    source.watermark = "source-2".into();
    source.messages[0].text = "I prefer spaces".into();
    candidate.body = "I prefer spaces".into();
    let second = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime
        .commit_extraction(&second, &source, &[candidate], now)
        .unwrap();
    let entries = runtime.list(ListMemoryRequest::default()).unwrap().data;
    assert_eq!(
        entries,
        vec![MemoryEntry {
            state: MemoryState::Conflicted,
            ..supported.clone()
        }]
    );
    let candidates = {
        let connection = runtime.connection.lock().unwrap();
        let mut statement = connection.prepare(
            "SELECT body, source_session_id, validation_outcome FROM memory_candidates ORDER BY body",
        ).unwrap();
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    assert_eq!(
        candidates,
        vec![
            (
                "I prefer spaces".into(),
                source.session_id.to_string(),
                "conflicted".into()
            ),
            ("I prefer tabs".into(), first_source_id, "accepted".into()),
        ]
    );
    assert!(
        runtime
            .search(crate::memory::SearchMemoryRequest {
                query: "tabs".into(),
                scope: MemoryScope::User,
                kind: None,
                state: None,
                workspace_root: std::path::PathBuf::new(),
            })
            .unwrap()
            .data
            .is_empty()
    );
    let restoration = remember_request("I prefer tabs");
    let mut provenance = supported.provenance.clone();
    provenance.push(MemoryProvenance {
        source_session_id: Some(restoration.source.session_id.to_string()),
        source_turn_id: restoration.source.turn_id.as_ref().map(ToString::to_string),
        source_user_item_id: restoration.source.user_item_id.clone(),
    });
    let restored = match runtime
        .execute_command(MemoryCommand::Remember(restoration))
        .await
        .unwrap()
    {
        MemoryCommandResult::Remember(entry) => entry,
        _ => panic!("remember result"),
    };
    assert_eq!(
        restored,
        MemoryEntry {
            origin: MemoryOrigin::ExplicitUser,
            state: MemoryState::Active,
            updated_at: restored.updated_at,
            provenance,
            ..supported
        }
    );
}

/// Trace: L2-DES-MEM-001 DD-9
/// Verifies: forgotten memory cannot be resurrected by a source predating revocation.
#[tokio::test]
async fn extracted_evidence_respects_durable_revocation() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let (source, candidate) = fixture();
    let entry = match runtime
        .execute_command(MemoryCommand::Remember(remember_request("I prefer tabs")))
        .await
        .unwrap()
    {
        MemoryCommandResult::Remember(entry) => entry,
        _ => panic!("remember result"),
    };
    let prepared = prepare_forget(
        &runtime,
        forget_request(MemoryForgetSelector::EntryId(entry.entry_id)),
    )
    .await
    .unwrap();
    runtime
        .execute_command(MemoryCommand::Forget(prepared))
        .await
        .unwrap();
    let now = Utc::now();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &source, &[candidate], now)
        .unwrap();
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data[0].state,
        MemoryState::Retired
    );
}

/// Trace: L2-DES-MEM-001 Operational Scheduling
/// Verifies: stale lease completion never writes entries or completes a replacement worker's job.
#[test]
fn stale_lease_cannot_commit_entries() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let (source, candidate) = fixture();
    let now = Utc::now();
    let old = runtime.claim_source(&source, now).unwrap().unwrap();
    let _new = runtime
        .claim_source(&source, now + Duration::minutes(3))
        .unwrap()
        .unwrap();
    runtime
        .commit_extraction(&old, &source, &[candidate], now + Duration::minutes(3))
        .unwrap();
    assert_eq!(runtime.status().unwrap().entry_count, 0);
}

/// Trace: L2-DES-MEM-001 DD-8
/// Verifies: source-proved inference does not downgrade or overwrite explicit authority.
#[tokio::test]
async fn inferred_equivalence_preserves_explicit_entry() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let (source, candidate) = fixture();
    runtime
        .execute_command(MemoryCommand::Remember(remember_request("I prefer tabs")))
        .await
        .unwrap();
    let now = Utc::now();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &source, &[candidate], now)
        .unwrap();
    let entries = runtime.list(ListMemoryRequest::default()).unwrap().data;
    assert_eq!(
        (
            entries.len(),
            entries[0].origin,
            entries[0].provenance.len()
        ),
        (1, MemoryOrigin::ExplicitUser, 2)
    );
}

/// Trace: L2-DES-MEM-001 DD-6/DD-8
/// Verifies: a failed evidence insert rolls back entries, candidates, FTS and job completion together.
#[test]
fn extraction_storage_failure_rolls_back_every_write() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let (source, candidate) = fixture();
    let now = Utc::now();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime.connection.lock().unwrap().execute_batch(
        "CREATE TRIGGER reject_evidence BEFORE INSERT ON memory_evidence BEGIN SELECT RAISE(ABORT, 'test failure'); END;"
    ).unwrap();
    assert!(
        runtime
            .commit_extraction(&claim, &source, &[candidate], now)
            .is_err()
    );
    let connection = runtime.connection.lock().unwrap();
    let counts = connection.query_row(
        "SELECT (SELECT COUNT(*) FROM memory_entries), (SELECT COUNT(*) FROM memory_candidates), (SELECT COUNT(*) FROM memory_evidence), (SELECT COUNT(*) FROM memory_entries_fts), (SELECT COUNT(*) FROM memory_proposal_claims), (SELECT state FROM memory_jobs)",
        [], |row| Ok((row.get::<_, u32>(0)?,row.get::<_, u32>(1)?,row.get::<_, u32>(2)?,row.get::<_, u32>(3)?,row.get::<_, u32>(4)?,row.get::<_, String>(5)?))
    ).unwrap();
    assert_eq!(counts, (0, 0, 0, 0, 0, "running".into()));
}

/// Trace: L2-DES-MEM-001 DD-6/DD-11
/// Verifies: projection failures preserve committed knowledge, surface a safe class, and cannot repeat extraction.
#[test]
fn projection_failure_is_visible_without_repeating_extraction() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let (source, candidate) = fixture();
    let now = Utc::now();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    std::fs::create_dir_all(root.path().join("user/MEMORY.md")).unwrap();
    assert!(
        runtime
            .commit_extraction(&claim, &source, &[candidate], now)
            .is_err()
    );
    let status = runtime.status().unwrap();
    assert_eq!(
        (
            status.entry_count,
            status.error_job_count,
            status.error_classes
        ),
        (1, 1, vec!["projection_error".to_string()])
    );
    assert_eq!(
        runtime
            .claim_source(&source, now + Duration::hours(1))
            .unwrap(),
        None
    );
    std::fs::remove_dir(root.path().join("user/MEMORY.md")).unwrap();
    drop(runtime);
    let repaired = open_runtime(root.path());
    assert_eq!(repaired.status().unwrap().entry_count, 1);
    assert!(
        std::fs::read_to_string(root.path().join("user/MEMORY.md"))
            .unwrap()
            .contains("I prefer tabs")
    );
}

/// Trace: L2-DES-MEM-001 DD-9
/// Verifies: explicit restoration does not allow a newly active session to renew its older revoked evidence.
#[tokio::test]
async fn restoration_does_not_renew_old_evidence() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let (mut source, candidate) = fixture();
    let entry = match runtime
        .execute_command(MemoryCommand::Remember(remember_request("I prefer tabs")))
        .await
        .unwrap()
    {
        MemoryCommandResult::Remember(entry) => entry,
        _ => panic!("remember result"),
    };
    let prepared = prepare_forget(
        &runtime,
        forget_request(MemoryForgetSelector::EntryId(entry.entry_id)),
    )
    .await
    .unwrap();
    runtime
        .execute_command(MemoryCommand::Forget(prepared))
        .await
        .unwrap();
    runtime
        .execute_command(MemoryCommand::Remember(remember_request("I prefer tabs")))
        .await
        .unwrap();
    let before = runtime.list(ListMemoryRequest::default()).unwrap().data;
    let now = Utc::now() + Duration::hours(8);
    source.observed_at = now - Duration::hours(7);
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &source, &[candidate], now)
        .unwrap();
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data,
        before
    );
}
/// Trace: L2-DES-MEM-001 DD-8/DD-9, Revision 4 structured-token identity
/// Verifies: a lossy legacy tombstone cannot suppress a distinct structured claim without exact-body proof.
#[tokio::test]
async fn legacy_key_collision_does_not_revoke_distinct_structured_claim() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let (mut source, mut candidate) = fixture();
    let entry = match runtime
        .execute_command(MemoryCommand::Remember(remember_request("foo1")))
        .await
        .unwrap()
    {
        MemoryCommandResult::Remember(entry) => entry,
        _ => panic!("remember result"),
    };
    let prepared = prepare_forget(
        &runtime,
        forget_request(MemoryForgetSelector::EntryId(entry.entry_id)),
    )
    .await
    .unwrap();
    runtime
        .execute_command(MemoryCommand::Forget(prepared))
        .await
        .unwrap();
    let now = Utc::now() + Duration::hours(8);
    source.observed_at = now - Duration::hours(7);
    source.messages[0].observed_at = source.observed_at;
    source.messages[0].text = "FOO=1".into();
    candidate.body = "FOO=1".into();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &source, &[candidate], now)
        .unwrap();
    let entries = runtime.list(ListMemoryRequest::default()).unwrap().data;
    assert_eq!(
        entries
            .into_iter()
            .map(|entry| (entry.body, entry.state))
            .collect::<Vec<_>>(),
        vec![
            ("FOO=1".to_string(), MemoryState::Active),
            ("foo1".to_string(), MemoryState::Retired)
        ]
    );
}

/// Trace: L2-DES-MEM-001 DD-6
/// Verifies: transactional admission also rejects a credential when parser validation is bypassed.
#[test]
fn short_credential_assignments_never_commit() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let (source, mut candidate) = fixture();
    candidate.body = "client_secret=x".into();
    let now = Utc::now();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &source, &[candidate], now)
        .unwrap();
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data,
        vec![]
    );
    let connection = runtime.connection.lock().unwrap();
    let counts = connection.query_row(
        "SELECT (SELECT COUNT(*) FROM memory_candidates), (SELECT COUNT(*) FROM memory_evidence), (SELECT COUNT(*) FROM memory_entries_fts), (SELECT state FROM memory_jobs)",
        [], |row| Ok((row.get::<_, u32>(0)?,row.get::<_, u32>(1)?,row.get::<_, u32>(2)?,row.get::<_, String>(3)?)),
    ).unwrap();
    assert_eq!(counts, (0, 0, 0, "completed".into()));
}
