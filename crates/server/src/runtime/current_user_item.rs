use super::*;

#[derive(Debug, thiserror::Error)]
pub(super) enum CurrentUserItemError {
    #[error("memory tool requires an active turn with a current user message")]
    ActiveTurnUnavailable,
    #[error("memory tool turn context does not match the active turn")]
    TurnMismatch,
    #[error("memory tool source must be the current user message")]
    ItemMismatch,
}

impl ServerRuntime {
    pub(super) async fn current_user_item_text(
        &self,
        session_id: SessionId,
        turn_id: TurnId,
        user_item_id: &devo_protocol::native::ids::ItemId,
    ) -> Result<String, CurrentUserItemError> {
        let stream = self
            .active_stream_state(session_id)
            .await
            .ok_or(CurrentUserItemError::ActiveTurnUnavailable)?;
        let stream = stream.lock().await;
        let inline = stream
            .turn_inline
            .as_ref()
            .ok_or(CurrentUserItemError::ActiveTurnUnavailable)?;
        if inline.turn_id != turn_id {
            return Err(CurrentUserItemError::TurnMismatch);
        }
        inline
            .persisted_turn_items
            .iter()
            .find_map(|item| {
                (item.turn_id == turn_id && item.item_id.to_string() == user_item_id.as_str())
                    .then(|| match &item.turn_item {
                        devo_core::TurnItem::UserMessage(text) => Some(text.text.clone()),
                        _ => None,
                    })
                    .flatten()
            })
            .ok_or(CurrentUserItemError::ItemMismatch)
    }
}
