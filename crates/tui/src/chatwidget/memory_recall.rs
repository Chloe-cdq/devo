//! Recall rows share the same cell in live and restored transcript views.

use crate::memory_recall::{MemoryRecall, MemoryRecallCell};

use super::ChatWidget;

impl ChatWidget {
    pub(super) fn on_memory_recall(&mut self, recall: MemoryRecall) {
        if self.history.iter().any(|cell| {
            cell.as_any()
                .downcast_ref::<MemoryRecallCell>()
                .is_some_and(|cell| cell.recall.item_id == recall.item_id)
        }) {
            return;
        }
        self.flush_active_cell();
        self.add_history_entry_without_redraw(Box::new(MemoryRecallCell { recall }));
    }
}
