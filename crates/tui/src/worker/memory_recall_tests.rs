use pretty_assertions::assert_eq;

use devo_protocol::native::ids;
use devo_protocol::native::item::{Item, ItemEnvelope, ItemState};
use devo_protocol::native::rpc_memory::{MemoryKind, MemoryRecallEntry, MemoryScope};

use super::*;

fn recall_envelope() -> ItemEnvelope {
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-12T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    ItemEnvelope {
        id: ids::ItemId::from_string(devo_core::ItemId::new().to_string()),
        session_id: ids::SessionId::from_string(SessionId::new().to_string()),
        turn_id: ids::TurnId::from_string(TurnId::new().to_string()),
        seq: 1,
        revision: 1,
        created_at: now,
        updated_at: now,
        state: ItemState::Completed,
        item: Item::MemoryRecall {
            snapshot_revision: "snapshot-1".into(),
            entries: vec![MemoryRecallEntry {
                entry_id: ids::MemoryEntryId::from_string("memory-1".into()),
                scope: MemoryScope::Project,
                kind: MemoryKind::Preference,
                summary: "Prefer Rust".into(),
                source_summary: "Explicit user; 1 evidence source".into(),
            }],
        },
    }
}

/// Trace: L1-REQ-MEM-001 Rev 2 Acceptance, L2-DES-MEM-001 Rev 4 DD-11.
/// Verifies: a completed Native recall item reaches the live TUI display instead of being dropped.
#[test]
fn memory_recall_completion_reaches_live_display() {
    let envelope = recall_envelope();
    let item_id = devo_core::ItemId::try_from(envelope.id.as_str()).unwrap();
    let expected = crate::memory_recall::MemoryRecall {
        item_id: envelope.id.clone(),
        snapshot_revision: "snapshot-1".into(),
        entries: vec![MemoryRecallEntry {
            entry_id: ids::MemoryEntryId::from_string("memory-1".into()),
            scope: MemoryScope::Project,
            kind: MemoryKind::Preference,
            summary: "Prefer Rust".into(),
            source_summary: "Explicit user; 1 evidence source".into(),
        }],
    };
    let payload = devo_protocol::TypedItemEventPayload {
        context: devo_protocol::EventContext {
            session_id: SessionId::try_from(envelope.session_id.as_str()).unwrap(),
            turn_id: Some(TurnId::try_from(envelope.turn_id.as_str()).unwrap()),
            item_id: Some(item_id),
            seq: 1,
            item_seq: Some(1),
        },
        item: envelope,
    };
    let (tx, mut rx) = mpsc::unbounded_channel();
    item_dispatch::dispatch_typed_item_lifecycle("item/completed", &payload, item_id, &tx);
    assert_eq!(
        rx.try_recv().unwrap(),
        WorkerEvent::MemoryRecallCompleted { recall: expected }
    );
    assert_eq!(rx.try_recv().is_err(), true);
}

/// Trace: L1-REQ-MEM-001 Rev 2 Acceptance, L2-DES-MEM-001 Rev 4 DD-11.
/// Verifies: a retained Native recall item survives session history restoration.
#[test]
fn memory_recall_survives_history_restore() {
    let envelope = recall_envelope();
    let expected = crate::memory_recall::RestoredMemoryRecall {
        before_index: 0,
        recall: crate::memory_recall::MemoryRecall {
            item_id: envelope.id.clone(),
            snapshot_revision: "snapshot-1".into(),
            entries: vec![MemoryRecallEntry {
                entry_id: ids::MemoryEntryId::from_string("memory-1".into()),
                scope: MemoryScope::Project,
                kind: MemoryKind::Preference,
                summary: "Prefer Rust".into(),
                source_summary: "Explicit user; 1 evidence source".into(),
            }],
        },
    };
    let restored =
        history::restored_history_items(Vec::new(), vec![envelope], CollaborationMode::Build);
    assert_eq!(
        restored,
        history::RestoredHistory {
            items: Vec::new(),
            memory_recalls: vec![expected],
        }
    );
}
