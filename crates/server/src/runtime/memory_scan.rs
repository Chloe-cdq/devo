use super::{ServerRuntime, SessionId};
use crate::memory::scan::{MemorySourceWork, ScanContext, SourceActivity};
use std::sync::{Arc, Weak};

struct RuntimeSourceActivity(Weak<ServerRuntime>);
#[async_trait::async_trait]
impl SourceActivity for RuntimeSourceActivity {
    async fn is_active(&self, session_id: SessionId) -> bool {
        match self.0.upgrade() {
            Some(runtime) => runtime.runtime_active_turn_id(session_id).await.is_some(),
            None => true,
        }
    }
}
impl ServerRuntime {
    pub(super) fn schedule_memory_scan(
        self: &Arc<Self>,
        triggering_session: SessionId,
        model_context: Arc<crate::session_context::SessionRuntimeContext>,
    ) {
        let Some(memory) = self.memory.as_ref().map(Arc::clone) else {
            return;
        };
        let context = ScanContext {
            db: Arc::clone(&self.deps.db),
            model_context,
            usage_ledger: self.usage_ledger.clone(),
            triggering_session,
            activity: Arc::new(RuntimeSourceActivity(Arc::downgrade(self))),
        };
        memory.enqueue_source(MemorySourceWork::Scan(context));
    }
}
