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
