use devo_protocol::native::item::ItemEnvelope;
use pretty_assertions::assert_eq;

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-6
/// Verifies: Native memory recall items retain a bounded inspection wire shape.
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

/// Trace: L2-DES-MEM-001 Rev 4 DD-6, DD-12.
/// Verifies: persisted recall completion events replay the full bounded Native item without transcript fields.
#[test]
fn memory_recall_completion_event_preserves_replay_contract() {
    use devo_protocol::native::event::EventEnvelope;

    let wire = serde_json::json!({
        "event": {
            "eventId": "evt_recall", "streamId": "session:ses_root", "seq": 8,
            "emittedAt": "2026-09-28T00:00:00Z", "persisted": true, "schemaVersion": 1
        },
        "notification": {
            "method": "item/completed",
            "params": {"item": {
                "id": "item_recall", "sessionId": "ses_root", "turnId": "turn_root", "seq": 7,
                "revision": 1, "createdAt": "2026-09-28T00:00:00Z", "updatedAt": "2026-09-28T00:00:00Z",
                "state": "completed", "item": {
                    "type": "memoryRecall", "snapshotRevision": "revision-1", "entries": [{
                        "entryId": "mem_tabs", "scope": "project", "kind": "preference",
                        "summary": "Use tabs", "sourceSummary": "Explicit user memory (1 source)"
                    }]
                }
            }}
        }
    });
    let event: EventEnvelope = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(serde_json::to_value(event).unwrap(), wire);
}
