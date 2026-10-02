use devo_core::InternalRecordV2;
use devo_core::ParsedRolloutLine;
use devo_core::parse_rollout_line;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::RolloutStore;

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: an external-context fact is written once and survives store restart.
#[test]
fn external_context_fact_is_durable_and_idempotent() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("session.jsonl");
    let session_id = devo_core::SessionId::new();
    let store = RolloutStore::new(dir.path().to_path_buf(), /*event_log*/ None);
    store
        .mark_external_context_used_at(&path, session_id)
        .unwrap();
    store
        .mark_external_context_used_at(&path, session_id)
        .unwrap();
    let restarted = RolloutStore::new(dir.path().to_path_buf(), /*event_log*/ None);
    restarted
        .mark_external_context_used_at(&path, session_id)
        .unwrap();

    let lines = std::fs::read_to_string(&path).unwrap();
    let facts = lines
        .lines()
        .map(|line| parse_rollout_line(line).unwrap())
        .filter(|line| {
            matches!(line, ParsedRolloutLine::V2(v2)
            if matches!(v2.as_ref(), devo_core::RolloutLineV2::Internal {
                entry: InternalRecordV2::ExternalContextUsed, ..
            }))
        })
        .count();
    assert_eq!(facts, 1);
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: a failed marker retains provenance intent outside optional memory and precedes the next acknowledged ordinary append.
#[test]
fn failed_external_marker_is_retried_by_ordinary_append() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("session.jsonl");
    let session = devo_core::SessionId::new();
    let db = std::sync::Arc::new(crate::db::Database::open(dir.path().join("devo.db")).unwrap());
    let store = RolloutStore::new(dir.path().to_path_buf(), Some(std::sync::Arc::clone(&db)));
    std::fs::create_dir(&path).unwrap();
    assert!(store.mark_external_context_used_at(&path, session).is_err());
    std::fs::remove_dir(&path).unwrap();
    store
        .append_goal_state(&path, session, /*goal*/ None)
        .unwrap();
    let lines = std::fs::read_to_string(&path).unwrap();
    let first = parse_rollout_line(lines.lines().next().unwrap()).unwrap();
    assert!(matches!(first, ParsedRolloutLine::V2(v2)
    if matches!(v2.as_ref(), devo_core::RolloutLineV2::Internal {
        entry: InternalRecordV2::ExternalContextUsed, ..
    })));
    assert_eq!(db.projection_watermark(&path).unwrap(), Some(1));
    assert_eq!(db.event_log_len().unwrap(), 0);
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: accepted large rollout rows cannot prevent recovery of durable external-context facts.
#[test]
fn external_context_recovery_accepts_large_rollout_rows() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("sessions/session.jsonl");
    let session = devo_core::SessionId::new();
    let store = RolloutStore::new(dir.path().to_path_buf(), /*event_log*/ None);
    store.mark_external_context_used_at(&path, session).unwrap();
    store
        .append_goal_state(
            &path,
            session,
            Some(serde_json::json!({
                "payload": "a".repeat(1024 * 1024 + 1)
            })),
        )
        .unwrap();
    assert_eq!(
        store.external_context_sources().unwrap(),
        std::collections::HashSet::from([session])
    );
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: a crash tail quarantines its known source while allowing unrelated provenance recovery to finish.
#[test]
fn external_context_recovery_isolates_known_crash_tail_source() {
    use std::io::Write;
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("sessions/session.jsonl");
    let session = devo_core::SessionId::new();
    let store = RolloutStore::new(dir.path().to_path_buf(), /*event_log*/ None);
    store
        .append_goal_state(&path, session, /*goal*/ None)
        .unwrap();
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"{\"v\":2,\"kind\":")
        .unwrap();
    assert_eq!(
        store.external_context_sources().unwrap(),
        std::collections::HashSet::from([session])
    );
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: newer unsupported rollout versions quarantine their source instead of reopening existing inferred memory.
#[test]
fn external_context_recovery_quarantines_unsupported_versions() {
    use std::io::Write;
    for (version, kind, record_type) in [
        (3, "internal", "externalContextUsed"),
        (2, "unknownFutureRow", "externalContextUsed"),
        (2, "internal", "unknownFutureProvenance"),
    ] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("sessions/session.jsonl");
        let session = devo_core::SessionId::new();
        let store = RolloutStore::new(dir.path().to_path_buf(), /*event_log*/ None);
        store
            .append_goal_state(&path, session, /*goal*/ None)
            .unwrap();
        let newer = serde_json::json!({"v": version, "kind": kind, "sessionId": session.to_string(),
            "entry": {"type": record_type}});
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        serde_json::to_writer(&mut file, &newer).unwrap();
        file.write_all(b"\n").unwrap();
        drop(file);
        assert_eq!(
            store.external_context_sources().unwrap(),
            std::collections::HashSet::from([session])
        );
    }
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: a frozen legacy session header identifies a crash-tail source without closing recovery for unrelated sessions.
#[test]
fn legacy_crash_tail_is_quarantined_by_source_identity() {
    use std::io::Write;
    let root = TempDir::new().unwrap();
    let store = RolloutStore::new(root.path().to_path_buf(), /*event_log*/ None);
    let record = store.create_session_record(
        devo_core::SessionId::new(),
        chrono::Utc::now(),
        root.path().to_path_buf(),
        Vec::new(),
        /*title*/ None,
        /*model*/ None,
        /*model_binding_id*/ None,
        /*reasoning_effort_selection*/ None,
        "test".into(),
        /*parent_session_id*/ None,
    );
    std::fs::create_dir_all(record.rollout_path.parent().unwrap()).unwrap();
    let mut file = std::fs::File::create(&record.rollout_path).unwrap();
    serde_json::to_writer(
        &mut file,
        &devo_core::RolloutLine::SessionMeta(Box::new(devo_core::SessionMetaLine {
            timestamp: chrono::Utc::now(),
            session: record.clone(),
        })),
    )
    .unwrap();
    file.write_all(b"\n{\"v\":2,\"kind\":").unwrap();
    drop(file);
    assert_eq!(
        store.external_context_sources().unwrap(),
        std::collections::HashSet::from([record.id])
    );
}

/// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
/// Verifies: frozen legacy turn and item identities localize damaged histories even without a session header.
#[test]
fn legacy_nested_identity_is_quarantined_without_session_header() {
    let fixture = include_str!("../../../core/tests/fixtures/rollout_v1/basic_session.jsonl");
    let session = "00000000-0000-0000-0000-0000000000b1".parse().unwrap();
    for row in fixture.lines().skip(1).take(2) {
        let root = TempDir::new().unwrap();
        let store = RolloutStore::new(root.path().to_path_buf(), /*event_log*/ None);
        std::fs::create_dir(root.path().join("sessions")).unwrap();
        std::fs::write(
            root.path().join("sessions/legacy.jsonl"),
            format!("{row}\n{{\"v\":2,\"kind\":"),
        )
        .unwrap();
        assert_eq!(
            store.external_context_sources().unwrap(),
            std::collections::HashSet::from([session])
        );
    }
}
