//! Safe Native recall snapshots shown as compact rows and full Ctrl+T records.

use ratatui::style::Stylize;
use ratatui::text::Line;

use devo_protocol::native::ids::ItemId;
use devo_protocol::native::item::{Item, ItemEnvelope};
use devo_protocol::native::rpc_memory::{MemoryKind, MemoryRecallEntry, MemoryScope};

use crate::history_cell::HistoryCell;
use crate::wrapping::{RtOptions, adaptive_wrap_lines};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MemoryRecall {
    pub(crate) item_id: ItemId,
    pub(crate) snapshot_revision: String,
    pub(crate) entries: Vec<MemoryRecallEntry>,
}

impl MemoryRecall {
    pub(crate) fn from_envelope(envelope: &ItemEnvelope) -> Option<Self> {
        let Item::MemoryRecall {
            snapshot_revision,
            entries,
        } = &envelope.item
        else {
            return None;
        };
        Some(Self {
            item_id: envelope.id.clone(),
            snapshot_revision: snapshot_revision.clone(),
            entries: entries.clone(),
        })
    }
}

/// Position relative to the existing rich display history, without changing legacy wire types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RestoredMemoryRecall {
    pub(crate) before_index: usize,
    pub(crate) recall: MemoryRecall,
}

#[derive(Debug)]
pub(crate) struct MemoryRecallCell {
    pub(crate) recall: MemoryRecall,
}

impl HistoryCell for MemoryRecallCell {
    fn display_lines(&self, width: u16) -> Vec<Line<'static>> {
        let count = self.recall.entries.len();
        let noun = if count == 1 { "memory" } else { "memories" };
        adaptive_wrap_lines(
            vec![Line::from(format!("Recalled {count} {noun} · Ctrl+T to view")).dim()],
            RtOptions::new(usize::from(width.max(1))),
        )
    }

    fn transcript_lines(&self, width: u16) -> Vec<Line<'static>> {
        let count = self.recall.entries.len();
        let noun = if count == 1 { "memory" } else { "memories" };
        let mut lines = vec![
            Line::from(format!("Recalled {count} {noun}")).bold(),
            Line::from("Advisory context for this turn.").dim(),
        ];
        for entry in &self.recall.entries {
            let scope = match entry.scope {
                MemoryScope::User => "User",
                MemoryScope::Project => "Project",
            };
            let kind = match entry.kind {
                MemoryKind::Preference => "Preference",
                MemoryKind::Feedback => "Feedback",
                MemoryKind::Fact => "Fact",
                MemoryKind::Reference => "Reference",
            };
            lines.extend([
                Line::from(""),
                Line::from(format!("{scope} · {kind} · {}", entry.entry_id)).dim(),
                Line::from(entry.summary.clone()),
                Line::from(entry.source_summary.clone()).dim(),
            ]);
        }
        adaptive_wrap_lines(lines, RtOptions::new(usize::from(width.max(1))))
    }
}
