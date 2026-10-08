use std::sync::Arc;

use chrono::{Duration, Utc};
use devo_protocol::native::ids::{ItemId, MemoryEntryId, SessionId, TurnId};
use devo_protocol::native::rpc_memory::{MemoryKind, MemoryOrigin, MemoryScope};
use devo_protocol::native::session::MemorySetting;
use pretty_assertions::assert_eq;

use super::command_types::{PreparedMemoryForgetScope, PreparedMemoryForgetTarget};
use super::extraction::ExtractionCandidate;
use super::runtime_test_support::{open_runtime, remember_request};
use super::source::{ExtractableSource, SourceMessage};
use super::{
    ListMemoryRequest, MemoryCommand, MemoryCommandResult, PrepareMemoryRequest,
    PreparedMemoryForgetRequest, USER_SCOPE_ID,
};

fn source() -> (ExtractableSource, ExtractionCandidate) {
    let turn_id = TurnId::new();
    (
        ExtractableSource {
            session_id: SessionId::new(),
            workspace_root: Default::default(),
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
        },
        ExtractionCandidate {
            scope: MemoryScope::User,
            kind: MemoryKind::Preference,
            key: "indentation preference".into(),
            body: "I prefer tabs".into(),
            evidence: vec![turn_id],
        },
    )
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: a durable deletion intent fences an already claimed extraction before cleanup completes.
#[test]
fn pending_deletion_fences_in_flight_extraction() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(crate::db::Database::open(root.path().join("devo.db")).unwrap());
    let mut runtime = open_runtime(&root.path().join("memory"));
    runtime.attach_deletion_ledger(Arc::clone(&db));
    let (mut source, candidate) = source();
    let source_id = devo_protocol::SessionId::new();
    source.session_id = SessionId::from_legacy_uuid(source_id.into());
    let now = Utc::now();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    db.record_memory_source_deletions(&[source_id]).unwrap();

    runtime
        .commit_extraction(&claim, &source, &[candidate], now)
        .unwrap();
    let connection = runtime.connection.lock().unwrap();
    let entries: i64 = connection
        .query_row("SELECT COUNT(*) FROM memory_entries", [], |row| row.get(0))
        .unwrap();
    assert_eq!(entries, 0);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-6/DD-13.
/// Verifies: a source with pending deletion never receives a new extraction lease.
#[test]
fn pending_deletion_fences_new_claim() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(crate::db::Database::open(root.path().join("devo.db")).unwrap());
    let mut runtime = open_runtime(&root.path().join("memory"));
    runtime.attach_deletion_ledger(Arc::clone(&db));
    let (mut source, _) = source();
    let source_id = devo_protocol::SessionId::new();
    source.session_id = SessionId::from_legacy_uuid(source_id.into());
    db.record_memory_source_deletions(&[source_id]).unwrap();

    assert_eq!(runtime.claim_source(&source, Utc::now()).unwrap(), None);
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: volatile exclusion intent blocks new claims and an already running extraction before either database can persist the fence.
#[test]
fn volatile_external_intent_fences_claims_and_in_flight_extraction() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(&root.path().join("memory"));
    let (mut source, candidate) = source();
    let legacy_source = devo_protocol::SessionId::new();
    source.session_id = SessionId::from_legacy_uuid(legacy_source.into());
    let now = Utc::now();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime.begin_external_context_sources(&[legacy_source]);
    let mut newer = source.clone();
    newer.watermark = "source-2".into();
    assert_eq!(runtime.claim_source(&newer, now).unwrap(), None);
    runtime
        .commit_extraction(&claim, &source, &[candidate], now)
        .unwrap();
    let connection = runtime.connection.lock().unwrap();
    let entries: i64 = connection
        .query_row("SELECT COUNT(*) FROM memory_entries", [], |row| row.get(0))
        .unwrap();
    assert_eq!(entries, 0);
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: pending volatile provenance hides inferred memory from list, recall and direct read without hiding explicit memory.
#[tokio::test]
async fn volatile_external_intent_fences_reads_and_recall() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(&root.path().join("memory"));
    let (mut source, candidate) = source();
    let legacy_source = devo_protocol::SessionId::new();
    source.session_id = SessionId::from_legacy_uuid(legacy_source.into());
    let now = Utc::now();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &source, &[candidate], now)
        .unwrap();
    let inferred = runtime
        .list(ListMemoryRequest::default())
        .unwrap()
        .data
        .remove(0);
    let explicit = runtime
        .remember(remember_request("Keep tabs for scripts"))
        .unwrap();
    runtime.begin_external_context_sources(&[legacy_source]);
    assert_eq!(
        runtime
            .list(ListMemoryRequest::default())
            .unwrap()
            .data
            .iter()
            .map(|entry| entry.entry_id.clone())
            .collect::<Vec<_>>(),
        vec![explicit.entry_id.clone()]
    );
    assert!(matches!(
        runtime.read(super::ReadMemoryRequest {
            entry_id: inferred.entry_id,
            workspace_root: root.path().to_path_buf(),
        }),
        Err(super::MemoryError::InvalidRequest(_))
    ));
    let recalled = runtime
        .prepare_turn(PrepareMemoryRequest {
            query: "tabs scripts".into(),
            workspace_root: root.path().to_path_buf(),
            session_recall: MemorySetting::On,
        })
        .await
        .unwrap();
    assert_eq!(
        recalled
            .entries
            .iter()
            .map(|entry| entry.entry_id.clone())
            .collect::<Vec<_>>(),
        vec![explicit.entry_id]
    );
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: startup closes inferred access before canonical replay, then preserves safe sources after repairing the recovered exclusion.
#[test]
fn startup_source_recovery_gates_inference_until_canonical_facts_are_replayed() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(crate::db::Database::open(root.path().join("devo.db")).unwrap());
    let mut runtime = open_runtime(&root.path().join("memory"));
    let now = Utc::now();
    let (mut tainted, candidate) = source();
    let legacy_tainted = devo_protocol::SessionId::new();
    tainted.session_id = SessionId::from_legacy_uuid(legacy_tainted.into());
    let claim = runtime.claim_source(&tainted, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &tainted, &[candidate], now)
        .unwrap();
    let (safe, mut candidate) = source();
    candidate.key = "other preference".into();
    candidate.body = "I prefer spaces".into();
    let claim = runtime.claim_source(&safe, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &safe, &[candidate], now)
        .unwrap();
    let store =
        crate::persistence::RolloutStore::new(root.path().to_path_buf(), /*event_log*/ None);
    store
        .mark_external_context_used_at(&root.path().join("sessions/source.jsonl"), legacy_tainted)
        .unwrap();
    runtime.attach_deletion_ledger(db);
    runtime.attach_source_rollout_store(store);
    assert!(
        runtime
            .list(ListMemoryRequest::default())
            .unwrap()
            .data
            .is_empty()
    );
    let mut newer = safe.clone();
    newer.watermark = "source-2".into();
    assert_eq!(runtime.claim_source(&newer, now).unwrap(), None);
    runtime.reconcile_source_intents();
    assert_eq!(
        runtime
            .list_recallable(ListMemoryRequest::default())
            .unwrap()
            .data
            .iter()
            .map(|entry| entry.body.clone())
            .collect::<Vec<_>>(),
        vec!["I prefer spaces"]
    );
    assert!(runtime.claim_source(&newer, now).unwrap().is_some());
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: forget selectors cannot bypass the pending-intent inferred read fence.
#[test]
fn forget_hides_inferred_entries_while_source_intent_is_pending() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(crate::db::Database::open(root.path().join("devo.db")).unwrap());
    let mut runtime = open_runtime(&root.path().join("memory"));
    runtime.attach_deletion_ledger(Arc::clone(&db));
    let (mut source, candidate) = source();
    let source_id = devo_protocol::SessionId::new();
    source.session_id = SessionId::from_legacy_uuid(source_id.into());
    let now = Utc::now();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &source, &[candidate], now)
        .unwrap();
    let entry_id: String = runtime
        .connection
        .lock()
        .unwrap()
        .query_row("SELECT entry_id FROM memory_entries", [], |row| row.get(0))
        .unwrap();
    let matching_explicit = runtime
        .remember(remember_request("I prefer spaces"))
        .unwrap();
    let explicit = runtime
        .remember(remember_request("Keep keyboard shortcuts"))
        .unwrap();
    db.record_memory_source_deletions(&[source_id]).unwrap();

    for target in [
        PreparedMemoryForgetTarget::Text("prefer tabs".into()),
        PreparedMemoryForgetTarget::Exact(MemoryEntryId::from_string(entry_id)),
    ] {
        let request = PreparedMemoryForgetRequest {
            target,
            scope: PreparedMemoryForgetScope {
                scope: MemoryScope::User,
                scope_id: USER_SCOPE_ID.into(),
            },
            source_session_id: source_id,
        };
        assert!(runtime.forget(request).is_err());
    }
    let matching_result = runtime
        .forget(PreparedMemoryForgetRequest {
            target: PreparedMemoryForgetTarget::Text("prefer".into()),
            scope: PreparedMemoryForgetScope {
                scope: MemoryScope::User,
                scope_id: USER_SCOPE_ID.into(),
            },
            source_session_id: source_id,
        })
        .unwrap();
    assert_eq!(
        matching_result.forgotten.unwrap().entry_id,
        matching_explicit.entry_id
    );
    let explicit_result = runtime
        .forget(PreparedMemoryForgetRequest {
            target: PreparedMemoryForgetTarget::Exact(explicit.entry_id),
            scope: PreparedMemoryForgetScope {
                scope: MemoryScope::User,
                scope_id: USER_SCOPE_ID.into(),
            },
            source_session_id: source_id,
        })
        .unwrap();
    assert_eq!(
        explicit_result.forgotten.unwrap().origin,
        MemoryOrigin::ExplicitUser
    );
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: a pending source deletion hides provenance in every public entry response while explicit memory remains visible.
#[tokio::test]
async fn pending_deletion_hides_explicit_entry_provenance() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(crate::db::Database::open(root.path().join("devo.db")).unwrap());
    let mut runtime = open_runtime(&root.path().join("memory"));
    runtime.attach_deletion_ledger(Arc::clone(&db));
    let source_id = devo_protocol::SessionId::new();
    let mut request = remember_request("I prefer tabs");
    request.source.session_id = source_id;
    let entry = runtime.remember(request).unwrap();
    assert_eq!(entry.provenance.len(), 1);
    db.record_memory_source_deletions(&[source_id]).unwrap();

    let listed = runtime
        .execute_command(MemoryCommand::List(ListMemoryRequest::default()))
        .await
        .unwrap();
    let MemoryCommandResult::List(listed) = listed else {
        panic!("expected memory list");
    };
    assert_eq!(listed.data.len(), 1);
    assert_eq!(listed.data[0].entry_id, entry.entry_id);
    assert!(listed.data[0].provenance.is_empty());

    let recalled = runtime
        .prepare_turn(PrepareMemoryRequest {
            query: "tabs".into(),
            workspace_root: root.path().to_path_buf(),
            session_recall: MemorySetting::On,
        })
        .await
        .unwrap();
    assert_eq!(recalled.entries.len(), 1);
    assert_eq!(recalled.entries[0].entry_id, entry.entry_id);
    assert_eq!(recalled.entries[0].source_summary, "Explicit user memory");

    let forgotten = runtime
        .execute_command(MemoryCommand::Forget(PreparedMemoryForgetRequest {
            target: PreparedMemoryForgetTarget::Exact(entry.entry_id),
            scope: PreparedMemoryForgetScope {
                scope: MemoryScope::User,
                scope_id: USER_SCOPE_ID.into(),
            },
            source_session_id: source_id,
        }))
        .await
        .unwrap();
    let MemoryCommandResult::Forget(forgotten) = forgotten else {
        panic!("expected memory forget");
    };
    assert!(forgotten.forgotten.unwrap().provenance.is_empty());

    let mut request = remember_request("Keep keyboard shortcuts");
    request.source.session_id = devo_protocol::SessionId::new();
    let remembered = runtime
        .execute_command(MemoryCommand::Remember(request))
        .await
        .unwrap();
    let MemoryCommandResult::Remember(entry) = remembered else {
        panic!("expected memory remember");
    };
    assert!(entry.provenance.is_empty());
    let projection = root.path().join("memory/user/MEMORY.md");
    std::fs::remove_file(&projection).unwrap();
    std::fs::create_dir(&projection).unwrap();
    let failed_projection = runtime
        .execute_command(MemoryCommand::Forget(PreparedMemoryForgetRequest {
            target: PreparedMemoryForgetTarget::Exact(entry.entry_id),
            scope: PreparedMemoryForgetScope {
                scope: MemoryScope::User,
                scope_id: USER_SCOPE_ID.into(),
            },
            source_session_id: source_id,
        }))
        .await;
    let Err(super::MemoryError::ForgetCommitted { result, .. }) = failed_projection else {
        panic!("expected committed forget with failed projection");
    };
    assert!(result.forgotten.unwrap().provenance.is_empty());
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: foreground recall obeys the durable deletion fence for inferred memory.
#[tokio::test]
async fn pending_deletion_hides_inferred_entry_from_turn_recall() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(crate::db::Database::open(root.path().join("devo.db")).unwrap());
    let mut runtime = open_runtime(&root.path().join("memory"));
    runtime.attach_deletion_ledger(Arc::clone(&db));
    let (mut source, candidate) = source();
    let source_id = devo_protocol::SessionId::new();
    source.session_id = SessionId::from_legacy_uuid(source_id.into());
    let now = Utc::now();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &source, &[candidate], now)
        .unwrap();
    let request = PrepareMemoryRequest {
        query: "tabs".into(),
        workspace_root: root.path().to_path_buf(),
        session_recall: MemorySetting::On,
    };
    assert_eq!(
        runtime
            .prepare_turn(request.clone())
            .await
            .unwrap()
            .entries
            .len(),
        1
    );

    db.record_memory_source_deletions(&[source_id]).unwrap();
    assert!(
        runtime
            .prepare_turn(request)
            .await
            .unwrap()
            .entries
            .is_empty()
    );
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: excluded external evidence stays auditable but is not returned with explicit memory.
#[test]
fn external_source_exclusion_hides_explicit_entry_provenance() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let source_id = devo_protocol::SessionId::new();
    let mut request = remember_request("I prefer tabs");
    request.source.session_id = source_id;
    let entry = runtime.remember(request).unwrap();
    assert_eq!(entry.provenance.len(), 1);

    runtime.exclude_sources(&[source_id], Utc::now()).unwrap();
    let listed = runtime.list(ListMemoryRequest::default()).unwrap().data;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].entry_id, entry.entry_id);
    assert!(listed[0].provenance.is_empty());
    let evidence_count: i64 = runtime
        .connection
        .lock()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM memory_evidence", [], |row| row.get(0))
        .unwrap();
    assert_eq!(evidence_count, 1);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: a memory commit waiting on a reader cannot delay durable deletion intent.
#[test]
fn deletion_intent_does_not_wait_for_blocked_memory_commit() {
    use std::sync::mpsc;
    use std::time::{Duration as StdDuration, Instant};

    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(crate::db::Database::open(root.path().join("devo.db")).unwrap());
    let memory_root = root.path().join("memory");
    let mut runtime = open_runtime(&memory_root);
    runtime.attach_deletion_ledger(Arc::clone(&db));
    let (mut source, candidate) = source();
    let source_id = devo_protocol::SessionId::new();
    source.session_id = SessionId::from_legacy_uuid(source_id.into());
    let now = Utc::now();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    let runtime = Arc::new(runtime);
    let database_path = memory_root.join("memory.sqlite3");
    let reader = rusqlite::Connection::open(&database_path).unwrap();
    reader.execute_batch("BEGIN DEFERRED").unwrap();
    let _: i64 = reader
        .query_row("SELECT COUNT(*) FROM memory_entries", [], |row| row.get(0))
        .unwrap();

    let committing = {
        let runtime = Arc::clone(&runtime);
        std::thread::spawn(move || runtime.commit_extraction(&claim, &source, &[candidate], now))
    };
    let probe = rusqlite::Connection::open(&database_path).unwrap();
    probe.busy_timeout(StdDuration::ZERO).unwrap();
    let deadline = Instant::now() + StdDuration::from_secs(2);
    let mut commit_waiting = false;
    while Instant::now() < deadline {
        let result: rusqlite::Result<i64> =
            probe.query_row("SELECT COUNT(*) FROM memory_entries", [], |row| row.get(0));
        if matches!(result, Err(rusqlite::Error::SqliteFailure(_, _))) {
            commit_waiting = true;
            break;
        }
        std::thread::sleep(StdDuration::from_millis(10));
    }
    let (recorded_tx, recorded_rx) = mpsc::channel();
    let recording = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || {
            db.record_memory_source_deletions(&[source_id]).unwrap();
            recorded_tx.send(()).unwrap();
        })
    };
    let recorded_without_waiting = recorded_rx.recv_timeout(StdDuration::from_secs(1)).is_ok();
    reader.execute_batch("ROLLBACK").unwrap();
    committing.join().unwrap().unwrap();
    recording.join().unwrap();
    assert!(commit_waiting, "commit did not reach the reader lock");
    assert!(
        recorded_without_waiting,
        "deletion intent waited for memory commit"
    );

    runtime.reconcile_source_intents();
    let connection = runtime.connection.lock().unwrap();
    let state: String = connection
        .query_row("SELECT state FROM memory_entries", [], |row| row.get(0))
        .unwrap();
    let evidence: i64 = connection
        .query_row("SELECT COUNT(*) FROM memory_evidence", [], |row| row.get(0))
        .unwrap();
    assert_eq!(state, "retired");
    assert_eq!(evidence, 0);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7/DD-13, Built-in Agent Tools.
/// Verifies: on-demand reads obey pending source intents without hiding explicit memory bodies.
#[tokio::test]
async fn pending_source_intents_fence_on_demand_reads() {
    enum Intent {
        Deletion,
        External,
    }
    for intent in [Intent::Deletion, Intent::External] {
        let root = tempfile::tempdir().unwrap();
        let db = Arc::new(crate::db::Database::open(root.path().join("devo.db")).unwrap());
        let mut runtime = open_runtime(&root.path().join("memory"));
        runtime.attach_deletion_ledger(Arc::clone(&db));
        let (mut source, candidate) = source();
        let source_id = devo_protocol::SessionId::new();
        source.session_id = SessionId::from_legacy_uuid(source_id.into());
        let now = Utc::now();
        let claim = runtime.claim_source(&source, now).unwrap().unwrap();
        runtime
            .commit_extraction(&claim, &source, &[candidate], now)
            .unwrap();
        let inferred = runtime
            .list(ListMemoryRequest::default())
            .unwrap()
            .data
            .remove(0);
        let inferred_request = super::ReadMemoryRequest {
            entry_id: inferred.entry_id.clone(),
            workspace_root: root.path().to_path_buf(),
        };
        assert_eq!(
            runtime
                .execute_command(MemoryCommand::Read(inferred_request.clone()))
                .await
                .unwrap(),
            MemoryCommandResult::Read(devo_protocol::native::rpc_memory::MemoryReadEntry {
                entry_id: inferred.entry_id,
                scope: MemoryScope::User,
                kind: MemoryKind::Preference,
                state: devo_protocol::native::rpc_memory::MemoryState::Active,
                body: "I prefer tabs".into(),
                source_summary: "Inferred session memory (1 source)".into(),
            })
        );
        let mut request = remember_request("Keep keyboard shortcuts");
        request.source.session_id = source_id;
        let explicit = runtime.remember(request).unwrap();
        match intent {
            Intent::Deletion => db.record_memory_source_deletions(&[source_id]).unwrap(),
            Intent::External => runtime.begin_external_context_sources(&[source_id]),
        }

        let result = runtime
            .execute_command(MemoryCommand::Read(inferred_request))
            .await;
        assert!(matches!(result, Err(super::MemoryError::InvalidRequest(_))));
        assert_eq!(
            runtime
                .execute_command(MemoryCommand::Read(super::ReadMemoryRequest {
                    entry_id: explicit.entry_id.clone(),
                    workspace_root: root.path().to_path_buf(),
                }))
                .await
                .unwrap(),
            MemoryCommandResult::Read(devo_protocol::native::rpc_memory::MemoryReadEntry {
                entry_id: explicit.entry_id,
                scope: MemoryScope::User,
                kind: MemoryKind::Preference,
                state: devo_protocol::native::rpc_memory::MemoryState::Active,
                body: "Keep keyboard shortcuts".into(),
                source_summary: "Explicit user memory".into(),
            })
        );
    }
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7/DD-13, Built-in Agent Tools, Privacy and Authority.
/// Verifies: search retains query validation and secret filtering alongside the source-intent fence.
#[test]
fn search_preserves_validation_and_filters_during_source_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(crate::db::Database::open(root.path().join("devo.db")).unwrap());
    let mut runtime = open_runtime(&root.path().join("memory"));
    runtime.attach_deletion_ledger(Arc::clone(&db));
    let (mut source, candidate) = source();
    let source_id = devo_protocol::SessionId::new();
    source.session_id = SessionId::from_legacy_uuid(source_id.into());
    let now = Utc::now();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &source, &[candidate], now)
        .unwrap();
    let explicit = runtime
        .remember(remember_request("Keep tabs for scripts"))
        .unwrap();
    let secret = runtime
        .remember(remember_request("Keep tabs for snippets"))
        .unwrap();
    runtime
        .connection
        .lock()
        .unwrap()
        .execute(
            "UPDATE memory_entries SET body = 'tabs api_key=private-value' WHERE entry_id = ?1",
            [secret.entry_id.as_str()],
        )
        .unwrap();
    db.record_memory_source_deletions(&[source_id]).unwrap();
    let mut request = super::SearchMemoryRequest {
        query: "  tabs  ".into(),
        scope: MemoryScope::User,
        kind: None,
        state: None,
        workspace_root: root.path().to_path_buf(),
    };
    assert_eq!(
        runtime.search(request.clone()).unwrap(),
        devo_protocol::native::page::Page {
            data: vec![devo_protocol::native::rpc_memory::MemorySearchEntry {
                entry_id: explicit.entry_id,
                scope: MemoryScope::User,
                kind: MemoryKind::Preference,
                state: devo_protocol::native::rpc_memory::MemoryState::Active,
                summary: "Keep tabs for scripts".into(),
            }],
            next_cursor: None,
        }
    );
    for query in [" \n ".into(), "界".repeat(1025)] {
        request.query = query;
        assert!(matches!(
            runtime.search(request.clone()),
            Err(super::MemoryError::InvalidRequest(_))
        ));
    }
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: startup imports legacy external exclusions even when their ledger receipt was already reconciled, without new primary writes.
#[test]
fn startup_imports_reconciled_legacy_exclusion_without_primary_writes() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(crate::db::Database::open(root.path().join("devo.db")).unwrap());
    let mut runtime = open_runtime(&root.path().join("memory"));
    let (mut source, candidate) = source();
    let legacy_source = devo_protocol::SessionId::new();
    source.session_id = SessionId::from_legacy_uuid(legacy_source.into());
    let now = Utc::now();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &source, &[candidate], now)
        .unwrap();
    db.record_external_context_sources(&[legacy_source])
        .unwrap();
    db.finish_external_context_sources(&[legacy_source])
        .unwrap();
    let primary = rusqlite::Connection::open(root.path().join("devo.db")).unwrap();
    primary.execute_batch("CREATE TRIGGER no_exclusion_insert BEFORE INSERT ON memory_external_context_sources BEGIN SELECT RAISE(FAIL, 'legacy ledger read only'); END;
        CREATE TRIGGER no_exclusion_update BEFORE UPDATE ON memory_external_context_sources BEGIN SELECT RAISE(FAIL, 'legacy ledger read only'); END;").unwrap();
    runtime.attach_deletion_ledger(db);
    runtime.attach_source_rollout_store(crate::persistence::RolloutStore::new(
        root.path().to_path_buf(),
        /*event_log*/ None,
    ));
    runtime.reconcile_source_intents();
    assert!(
        runtime
            .list_recallable(ListMemoryRequest::default())
            .unwrap()
            .data
            .is_empty()
    );
    let mut newer = source;
    newer.watermark = "after-legacy-import".into();
    assert_eq!(runtime.claim_source(&newer, now).unwrap(), None);
}
