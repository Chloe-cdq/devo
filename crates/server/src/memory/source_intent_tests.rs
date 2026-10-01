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
            workspace_root: root.path().to_path_buf(),
            session_recall: MemorySetting::On,
        })
        .await
        .unwrap();
    assert_eq!(recalled.user_entries.len(), 1);
    assert!(recalled.user_entries[0].provenance.is_empty());

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
    request.source.session_id = source_id;
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
