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
    let store = RolloutStore::new(dir.path().to_path_buf(), /*event_log*/ None);
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
