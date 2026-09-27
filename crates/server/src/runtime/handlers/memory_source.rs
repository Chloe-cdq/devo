use super::super::*;

use crate::memory::MemorySourceBinding;

pub(super) struct ActiveMemoryMutationSource {
    pub(super) active_session_ids: Vec<devo_protocol::SessionId>,
    pub(super) source: Option<MemorySourceBinding>,
}

impl ServerRuntime {
    pub(super) async fn resolve_active_memory_mutation_source(
        &self,
        connection_id: u64,
        source_user_item_id: Option<&devo_protocol::native::ids::ItemId>,
        operation: &str,
        request_id: &serde_json::Value,
    ) -> Result<ActiveMemoryMutationSource, serde_json::Value> {
        let active_turns = self.active_turns.turns_for_connection(connection_id).await;
        let active_session_ids = active_turns
            .iter()
            .map(|(session_id, _)| *session_id)
            .collect::<Vec<_>>();
        if active_turns.is_empty() && source_user_item_id.is_some() {
            return Err(self.error_response(
                request_id.clone(),
                ProtocolErrorCode::InvalidParams,
                format!("direct {operation} commands must omit sourceUserItemId"),
            ));
        }
        let source = if let Some(source_user_item_id) = source_user_item_id {
            let mut source = None;
            for (session_id, turn) in &active_turns {
                if self
                    .current_user_item_text(*session_id, turn.turn_id, source_user_item_id)
                    .await
                    .is_ok()
                {
                    source = Some(MemorySourceBinding {
                        session_id: Some(*session_id),
                        turn_id: Some(turn.turn_id),
                        user_item_id: Some(source_user_item_id.clone()),
                    });
                    break;
                }
            }
            let Some(source) = source else {
                return Err(self.error_response(
                    request_id.clone(),
                    ProtocolErrorCode::InvalidParams,
                    format!("{operation} source item is not the current user message"),
                ));
            };
            Some(source)
        } else {
            None
        };
        Ok(ActiveMemoryMutationSource {
            active_session_ids,
            source,
        })
    }
}
