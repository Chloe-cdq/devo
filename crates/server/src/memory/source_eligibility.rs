//! Bounded, content-free source admission diagnostics.

use devo_protocol::native::rpc_memory::MemorySourceExclusionReason;

use super::MemoryRuntime;

impl MemoryRuntime {
    pub(super) fn note_source_exclusion(&self, reason: MemorySourceExclusionReason) {
        self.source_exclusion_reasons
            .lock()
            .expect("source eligibility state poisoned")
            .insert(reason);
    }
}
