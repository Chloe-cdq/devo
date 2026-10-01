use std::sync::Arc;

use chrono::{Duration, Utc};
use devo_protocol::native::ids::{ItemId, SessionId, TurnId};
use devo_protocol::native::rpc_memory::{MemoryKind, MemoryScope};
use devo_protocol::native::session::MemorySetting;
use pretty_assertions::assert_eq;

use super::extraction::ExtractionCandidate;
use super::runtime_test_support::open_runtime;
use super::source::{ExtractableSource, SourceMessage};

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
