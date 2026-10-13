use pretty_assertions::assert_eq;

use devo_protocol::native::ids;
use devo_protocol::native::rpc_memory::{MemoryKind, MemoryRecallEntry, MemoryScope};

use super::*;
use crate::events::WorkerEvent;
use crate::memory_recall::{MemoryRecall, RestoredMemoryRecall};

fn recall() -> MemoryRecall {
    MemoryRecall {
        item_id: ids::ItemId::from_string("recall-1".into()),
        snapshot_revision: "snapshot-1".into(),
        entries: vec![MemoryRecallEntry {
            entry_id: ids::MemoryEntryId::from_string("memory-1".into()),
            scope: MemoryScope::Project,
            kind: MemoryKind::Preference,
            summary: "Prefer Rust".into(),
            source_summary: "Explicit user; 1 evidence source".into(),
        }],
    }
}

fn restore_event(
    cwd: PathBuf,
    items: Vec<devo_protocol::SessionHistoryItem>,
    memory_recalls: Vec<RestoredMemoryRecall>,
) -> WorkerEvent {
    WorkerEvent::SessionSwitched {
        session_id: "session-1".into(),
        cwd,
        title: Some("Memory".into()),
        model: Some("test-model".into()),
        model_binding_id: None,
        reasoning_effort_selection: None,
        reasoning_effort: None,
        active_agent_label: None,
        total_input_tokens: 0,
        total_output_tokens: 0,
        total_tokens: 0,
        total_cache_read_tokens: 0,
        last_query_total_tokens: 0,
        last_query_input_tokens: 0,
        prompt_token_estimate: 0,
        history_items: Vec::new(),
        loaded_item_count: (items.len() + memory_recalls.len()) as u64,
        rich_history_items: items,
        memory_recalls,
        pending_texts: Vec::new(),
        collaboration_mode: CollaborationMode::Build,
        permission_preset: None,
        effective_context_window: None,
        last_context_occupancy: None,
    }
}

/// Trace: L1-REQ-MEM-001 Rev 2 Acceptance, L2-DES-MEM-001 Rev 4 DD-11.
/// Verifies: the live row is compact, its Ctrl+T view exposes provenance, and replay does not duplicate it.
#[test]
fn memory_recall_live_row_expands_in_transcript_once() {
    let workspace = tempfile::tempdir().unwrap();
    let (mut widget, _rx) = widget_with_model(Model::default(), workspace.path().into());
    let _ = widget.drain_scrollback_lines(/*width*/ 100);
    for _ in 0..2 {
        widget.handle_worker_event(WorkerEvent::MemoryRecallCompleted { recall: recall() });
    }
    let compact = scrollback_plain_lines(&widget.drain_scrollback_lines(/*width*/ 100))
        .into_iter()
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    let expanded = transcript_overlay_text(&widget, /*width*/ 100);
    assert_eq!(compact, vec!["Recalled 1 memory · Ctrl+T to view"]);
    assert_eq!(
        [
            expanded.matches("Recalled 1 memory").count() == 1,
            expanded.contains("Project · Preference · memory-1"),
            expanded.contains("Prefer Rust"),
            expanded.contains("Explicit user; 1 evidence source"),
            expanded.contains("Advisory context for this turn."),
        ],
        [true; 5]
    );
}

/// Trace: L1-REQ-MEM-001 Rev 2 Acceptance, L2-DES-MEM-001 Rev 4 DD-11.
/// Verifies: historical recall remains between the original user prompt and assistant reply.
#[test]
fn memory_recall_restored_row_preserves_turn_order() {
    let workspace = tempfile::tempdir().unwrap();
    let (mut widget, _rx) = widget_with_model(Model::default(), workspace.path().into());
    let items = vec![
        devo_protocol::SessionHistoryItem::new(
            /*tool_call_id*/ None,
            devo_protocol::SessionHistoryItemKind::User,
            "User".into(),
            "Build a tool".into(),
        ),
        devo_protocol::SessionHistoryItem::new(
            /*tool_call_id*/ None,
            devo_protocol::SessionHistoryItemKind::Assistant,
            "Assistant".into(),
            "Done".into(),
        ),
    ];
    widget.handle_worker_event(restore_event(
        workspace.path().into(),
        items,
        vec![RestoredMemoryRecall {
            before_index: 1,
            recall: recall(),
        }],
    ));
    let overlay = transcript_overlay_text(&widget, /*width*/ 100);
    assert_eq!(
        [
            overlay.find("Build a tool").unwrap() < overlay.find("Recalled 1 memory").unwrap(),
            overlay.find("Recalled 1 memory").unwrap() < overlay.find("Done").unwrap(),
            overlay.contains("memory-1"),
            overlay.contains("Explicit user; 1 evidence source"),
        ],
        [true; 4]
    );
}

/// Trace: L1-REQ-MEM-001 Rev 2 Acceptance, L2-DES-MEM-001 Rev 4 DD-11.
/// Verifies: an empty prepared snapshot is still inspectable after restoration.
#[test]
fn memory_recall_empty_snapshot_survives_restore() {
    let workspace = tempfile::tempdir().unwrap();
    let (mut widget, _rx) = widget_with_model(Model::default(), workspace.path().into());
    let mut empty = recall();
    empty.entries.clear();
    widget.handle_worker_event(restore_event(
        workspace.path().into(),
        Vec::new(),
        vec![RestoredMemoryRecall {
            before_index: 0,
            recall: empty,
        }],
    ));
    assert_eq!(
        transcript_overlay_text(&widget, /*width*/ 100).contains("Recalled 0 memories"),
        true
    );
}
