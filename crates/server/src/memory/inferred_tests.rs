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

/// Trace: L2-DES-MEM-001 DD-6
/// Verifies: direct commits validate both exact candidate fields before storing any copies.
#[test]
fn credential_assignment_variants_never_commit_keys_or_bodies() {
    for field in ["body", "key"] {
        for text in [
            "API key: \" \"",
            "password=;",
            "_API_KEY=ab",
            "_password=ab",
            "API key: ab",
            "API key = abcdefghijklmnop",
            "Credentials: API key: ab",
            "option = password=ab",
            "API\nkey=ab",
            "API\u{2003}key=ab",
            "API key:\nab",
            "API key:\u{2003}ab",
        ] {
            let root = tempfile::tempdir().unwrap();
            let runtime = open_runtime(root.path());
            let (source, mut candidate) = fixture();
            match field {
                "body" => candidate.body = text.into(),
                "key" => candidate.key = text.into(),
                _ => unreachable!(),
            }
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
                "SELECT (SELECT COUNT(*) FROM memory_candidates), (SELECT COUNT(*) FROM memory_evidence), (SELECT COUNT(*) FROM memory_entries_fts), (SELECT COUNT(*) FROM memory_proposal_claims)",
                [], |row| Ok((row.get::<_, u32>(0)?, row.get::<_, u32>(1)?, row.get::<_, u32>(2)?, row.get::<_, u32>(3)?)),
            ).unwrap();
            assert_eq!(counts, (0, 0, 0, 0));
        }
    }
}

/// Trace: L2-DES-MEM-001 DD-6
/// Verifies: Native explicit admission rejects spelling variants without persisting text.
#[tokio::test]
async fn credential_assignment_variants_reject_explicit_memory() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    for text in [
        "API key: \" \"",
        "password=;",
        "_API_KEY=ab",
        "_password=ab",
        "API key: ab",
        "API key = abcdefghijklmnop",
        "Credentials: API key: ab",
        "option = password=ab",
        "API\nkey=ab",
        "API\u{2003}key=ab",
        "API key:\nab",
        "API key:\u{2003}ab",
    ] {
        assert!(matches!(
            runtime
                .execute_command(MemoryCommand::Remember(remember_request(text)))
                .await,
            Err(crate::memory::MemoryError::SecretContentRejected)
        ));
        assert_eq!(
            runtime.list(ListMemoryRequest::default()).unwrap().data,
            vec![]
        );
    }
}

/// Trace: L2-DES-MEM-001 DD-6
/// Verifies: key normalization cannot turn a parser-bypassed proposal into persisted credential bytes.
#[test]
fn credential_normalized_proposal_keys_never_commit() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let (source, mut candidate) = fixture();
    candidate.key = "API\nkey=ab".into();
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
    assert_eq!(connection.query_row(
        "SELECT (SELECT COUNT(*) FROM memory_candidates), (SELECT COUNT(*) FROM memory_proposal_claims), (SELECT COUNT(*) FROM memory_entries_fts)",
        [], |row| Ok((row.get::<_, u32>(0)?,row.get::<_, u32>(1)?,row.get::<_, u32>(2)?))
    ).unwrap(), (0,0,0));
}

/// Trace: L2-DES-MEM-001 Entry Lifecycle and Retention
/// Verifies: a source deleted during extraction cannot be reclaimed from a stale scan snapshot.
#[test]
fn deleted_source_cannot_be_reclaimed_or_committed() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let (mut source, candidate) = fixture();
    source.session_id = SessionId::from_legacy_uuid(uuid::Uuid::new_v4());
    let now = Utc::now();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    let session_id = devo_protocol::SessionId::try_from(source.session_id.as_str()).unwrap();

    runtime.delete_sources(&[session_id], now).unwrap();
    assert_eq!(runtime.claim_source(&source, now).unwrap(), None);
    runtime
        .commit_extraction(&claim, &source, &[candidate], now)
        .unwrap();
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data,
        vec![]
    );

    drop(runtime);
    let reopened = open_runtime(root.path());
    assert_eq!(reopened.claim_source(&source, now).unwrap(), None);
}

/// Trace: L2-DES-MEM-001 Entry Lifecycle and Retention
/// Verifies: deleting one of two supporting sessions retains inferred memory until the last evidence is removed.
#[test]
fn deleting_sources_retires_only_after_final_evidence() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let (mut first, candidate) = fixture();
    first.session_id = SessionId::from_legacy_uuid(uuid::Uuid::new_v4());
    let now = Utc::now();
    let claim = runtime.claim_source(&first, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &first, std::slice::from_ref(&candidate), now)
        .unwrap();
    let mut second = first.clone();
    second.session_id = SessionId::from_legacy_uuid(uuid::Uuid::new_v4());
    second.watermark = "source-2".into();
    let claim = runtime.claim_source(&second, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &second, &[candidate], now)
        .unwrap();

    let first_id = devo_protocol::SessionId::try_from(first.session_id.as_str()).unwrap();
    runtime.delete_sources(&[first_id], now).unwrap();
    let remaining = runtime.list(ListMemoryRequest::default()).unwrap().data;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].state, MemoryState::Active);
    assert_eq!(
        remaining[0].provenance,
        vec![MemoryProvenance {
            source_session_id: Some(second.session_id.to_string()),
            source_turn_id: Some(second.messages[0].turn_id.to_string()),
            source_user_item_id: Some(second.messages[0].item_id.clone()),
        }]
    );

    let second_id = devo_protocol::SessionId::try_from(second.session_id.as_str()).unwrap();
    runtime.delete_sources(&[second_id], now).unwrap();
    let retired = runtime.list(ListMemoryRequest::default()).unwrap().data;
    assert_eq!(retired.len(), 1);
    assert_eq!(retired[0].state, MemoryState::Retired);
    let connection = runtime.connection.lock().unwrap();
    let fts_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM memory_entries_fts", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(fts_count, 0);
}

/// Trace: L2-DES-MEM-001 Storage Model
/// Verifies: expired raw candidates and completed job detail are removed without replaying the processed watermark.
#[test]
fn reopening_prunes_expired_detail_but_keeps_watermark_idempotent() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let (source, candidate) = fixture();
    let now = Utc::now();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &source, &[candidate], now)
        .unwrap();
    {
        let connection = runtime.connection.lock().unwrap();
        connection
            .execute(
                "UPDATE memory_candidates SET retention_until = '2020-01-01T00:00:00Z'",
                [],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE memory_jobs SET updated_at = '2020-01-01T00:00:00Z' WHERE state = 'completed'",
                [],
            )
            .unwrap();
    }
    drop(runtime);

    let reopened = open_runtime(root.path());
    let connection = reopened.connection.lock().unwrap();
    let counts: (i64, i64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM memory_candidates),
                    (SELECT COUNT(*) FROM memory_jobs WHERE state = 'completed')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(counts, (0, 0));
    drop(connection);
    assert_eq!(
        reopened.status().unwrap().last_successful_scan_at,
        Some(
            chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
                .unwrap()
                .to_utc()
        )
    );
    assert_eq!(reopened.claim_source(&source, now).unwrap(), None);
}

/// Trace: L2-DES-MEM-001 Entry Lifecycle and Retention
/// Verifies: retrying source deletion repairs a projection failure after the SQLite cleanup committed.
#[test]
fn retrying_source_deletion_repairs_projection_after_commit() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let (mut source, candidate) = fixture();
    source.session_id = SessionId::from_legacy_uuid(uuid::Uuid::new_v4());
    let now = Utc::now();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &source, &[candidate], now)
        .unwrap();
    let projection_dir = root.path().join("user");
    std::fs::remove_file(projection_dir.join("MEMORY.md")).unwrap();
    std::fs::remove_dir(&projection_dir).unwrap();
    std::fs::write(&projection_dir, "blocked").unwrap();
    let source_id = devo_protocol::SessionId::try_from(source.session_id.as_str()).unwrap();

    assert!(runtime.delete_sources(&[source_id], now).is_err());
    std::fs::remove_file(&projection_dir).unwrap();
    runtime.delete_sources(&[source_id], now).unwrap();
    let projection = std::fs::read_to_string(projection_dir.join("MEMORY.md")).unwrap();
    assert!(projection.contains("state: retired"));
    assert!(!projection.contains(source.session_id.as_str()));
}

/// Trace: L2-DES-MEM-001 Entry Lifecycle and Retention
/// Verifies: an unrelated broken projection does not block deletion of a source with no memory.
#[tokio::test]
async fn unrelated_projection_failure_does_not_block_source_deletion() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    runtime
        .execute_command(MemoryCommand::Remember(remember_request("I prefer tabs")))
        .await
        .unwrap();
    let projection_dir = root.path().join("user");
    std::fs::remove_file(projection_dir.join("MEMORY.md")).unwrap();
    std::fs::remove_dir(&projection_dir).unwrap();
    std::fs::write(&projection_dir, "blocked").unwrap();

    let source_id = devo_protocol::SessionId::new();
    runtime.delete_sources(&[source_id], Utc::now()).unwrap();
}

/// Trace: L2-DES-MEM-001 Entry Lifecycle and Retention
/// Verifies: fresh evidence can restore an entry retired only because its old source was deleted.
#[test]
fn fresh_source_reactivates_entry_retired_by_source_deletion() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let (mut first, candidate) = fixture();
    first.session_id = SessionId::from_legacy_uuid(uuid::Uuid::new_v4());
    let now = Utc::now();
    let claim = runtime.claim_source(&first, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &first, std::slice::from_ref(&candidate), now)
        .unwrap();
    let first_id = devo_protocol::SessionId::try_from(first.session_id.as_str()).unwrap();
    runtime.delete_sources(&[first_id], now).unwrap();

    let mut second = first.clone();
    second.session_id = SessionId::from_legacy_uuid(uuid::Uuid::new_v4());
    second.watermark = "source-2".into();
    let claim = runtime.claim_source(&second, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &second, &[candidate], now)
        .unwrap();
    let entries = runtime.list(ListMemoryRequest::default()).unwrap().data;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].state, MemoryState::Active);
    assert_eq!(
        entries[0].provenance,
        vec![MemoryProvenance {
            source_session_id: Some(second.session_id.to_string()),
            source_turn_id: Some(second.messages[0].turn_id.to_string()),
            source_user_item_id: Some(second.messages[0].item_id.clone()),
        }]
    );
    let connection = runtime.connection.lock().unwrap();
    let fts_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM memory_entries_fts", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(fts_count, 1);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: excluding one of two equivalent sources preserves clean support but hides excluded provenance.
#[test]
fn external_source_exclusion_keeps_only_clean_public_provenance() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let (mut clean, candidate) = fixture();
    clean.session_id = SessionId::from_legacy_uuid(uuid::Uuid::new_v4());
    let now = Utc::now();
    let claim = runtime.claim_source(&clean, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &clean, std::slice::from_ref(&candidate), now)
        .unwrap();
    let mut external = clean.clone();
    external.session_id = SessionId::from_legacy_uuid(uuid::Uuid::new_v4());
    external.watermark = "external".into();
    let claim = runtime.claim_source(&external, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &external, &[candidate], now)
        .unwrap();

    runtime
        .exclude_sources(
            &[devo_protocol::SessionId::try_from(external.session_id.as_str()).unwrap()],
            now,
        )
        .unwrap();

    let entries = runtime.list(ListMemoryRequest::default()).unwrap().data;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].state, MemoryState::Active);
    assert_eq!(entries[0].provenance.len(), 1);
    assert_eq!(
        entries[0].provenance[0].source_session_id.as_deref(),
        Some(clean.session_id.as_str())
    );
}
