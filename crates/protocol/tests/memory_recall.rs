use devo_protocol::native::item::ItemEnvelope;
use pretty_assertions::assert_eq;

#[test]
fn memory_recall_item_preserves_bounded_inspection_wire_shape() {
    let wire = serde_json::json!({
        "id": "item_recall", "sessionId": "ses_root", "turnId": "turn_root", "seq": 7,
        "revision": 1, "createdAt": "2026-09-28T00:00:00Z", "updatedAt": "2026-09-28T00:00:00Z",
        "state": "completed", "item": {
            "type": "memoryRecall", "snapshotRevision": "revision-1", "entries": [{
                "entryId": "mem_tabs", "scope": "project", "kind": "preference",
                "summary": "Use tabs", "sourceSummary": "Explicit user memory (1 source)"
            }]
        }
    });
    let item: ItemEnvelope = serde_json::from_value(wire.clone()).expect("Native recall item");
    assert_eq!(
        serde_json::to_value(item).expect("serialize recall item"),
        wire
    );
}
