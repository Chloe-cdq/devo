use std::collections::HashSet;

use devo_core::SessionId;
use devo_protocol::native::event::StreamSelector;

use super::ServerRuntime;

impl ServerRuntime {
    pub(super) async fn subscribed_session_for_connection(
        &self,
        connection_id: u64,
    ) -> Option<SessionId> {
        let connections = self.connections.lock().await;
        let connection = connections.get(&connection_id)?;
        let mut session_ids = connection
            .subscriptions
            .iter()
            .filter_map(|subscription| subscription.session_id);
        let session_id = session_ids.next()?;
        session_ids
            .all(|other| other == session_id)
            .then_some(session_id)
    }

    /// Returns all Session selectors registered through Native
    /// `subscription/*` calls for one connection. The subscription registry
    /// is authoritative because the connection cache stores only a delivery
    /// projection and may be rebuilt asynchronously around lifecycle events.
    pub(super) async fn native_session_ids_for_connection(
        &self,
        connection_id: u64,
    ) -> Vec<SessionId> {
        let subscriptions = self.event_subscriptions.lock().await;
        let mut session_ids = HashSet::new();
        for subscription in subscriptions
            .values()
            .filter(|subscription| subscription.connection_id == connection_id)
        {
            for selector in &subscription.selectors {
                if let StreamSelector::Session { session_id } = selector
                    && let Ok(session_id) = SessionId::try_from(session_id.as_str())
                {
                    session_ids.insert(session_id);
                }
            }
        }
        let mut session_ids = session_ids.into_iter().collect::<Vec<_>>();
        session_ids.sort_unstable_by_key(|session_id| session_id.to_string());
        session_ids
    }
}
