use super::read_source;
use super::tests::{legacy, write_lines};
use pretty_assertions::assert_eq;
use serde_json::json;

/// Trace: L2-DES-MEM-001 DD-7, DD-9. Background usage remains persisted without changing source identity or idle age.
#[test]
fn memory_accounting_does_not_change_extractable_source() {
    let root = tempfile::tempdir().unwrap();
    let mut lines = legacy(root.path());
    let before = read_source(&write_lines(&root, &lines)).unwrap().unwrap();
    let mut usage = json!({"v":2,"kind":"internal","timestamp":"2026-07-02T13:00:00Z",
        "sessionId":"00000000-0000-0000-0000-0000000000b1","turnId":null,"seq":0,
        "entry":{"type":"usageRecord","record":{
            "callId":"memory-call","sessionId":"00000000-0000-0000-0000-0000000000b1",
            "turnId":null,"purpose":"memoryExtraction","model":{"provider":"test","model":"test-fast"},
            "outcome":"succeeded","recordedAt":"2026-07-02T13:00:00Z"}}});
    lines.push(usage.clone());
    let path = write_lines(&root, &lines);
    let mut expected = before.clone();
    use sha2::{Digest, Sha256};
    expected.legacy_watermarks = vec![format!(
        "{:x}",
        Sha256::digest(std::fs::read(&path).unwrap())
    )];
    assert_eq!(read_source(&path).unwrap(), Some(expected));
    usage["entry"]["record"]["purpose"] = json!("turnQuery");
    *lines.last_mut().unwrap() = usage;
    let active = read_source(&write_lines(&root, &lines)).unwrap().unwrap();
    assert_ne!(active.watermark, before.watermark);
    assert_eq!(
        active.observed_at,
        "2026-07-02T13:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap()
    );
    lines.push(
        json!({"v":2,"kind":"internal","timestamp":"2026-07-02T14:00:00Z",
        "sessionId":"00000000-0000-0000-0000-0000000000b1","turnId":null,"seq":0,
        "entry":{"type":"externalContextUsed"}}),
    );
    assert_eq!(read_source(&write_lines(&root, &lines)).unwrap(), None);
}
