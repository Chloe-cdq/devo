use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use devo_core::tools::{MemoryToolInvocation, ToolCallError};
use devo_protocol::native::ids::MemoryEntryId;
use devo_protocol::native::rpc_memory::{MemoryEntry, MemoryScope, MemorySearchEntry};

const PENDING_SELECTION_TTL: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingForgetCandidate {
    entry_id: MemoryEntryId,
    scope: MemoryScope,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingForgetSelection {
    selection_id: u64,
    source_turn_id: devo_protocol::TurnId,
    source_user_item_id: devo_protocol::native::ids::ItemId,
    candidates: Vec<PendingForgetCandidate>,
    expires_at: Instant,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ForgetState {
    pending_by_session: HashMap<devo_protocol::SessionId, PendingForgetSelection>,
    active: Option<ActiveForgetMutation>,
    next_reservation_id: u64,
    next_selection_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveForgetMutation {
    reservation_id: u64,
    session_id: devo_protocol::SessionId,
    kind: ActiveForgetKind,
    entry_id: Option<MemoryEntryId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveForgetKind {
    AgentDirect,
    AgentConfirmed { selection_id: u64 },
    Native,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ForgetCommandGrammar {
    Direct,
    Confirmation,
}

/// Coordinates every server memory deletion through one fail-fast lease.
///
/// Search candidates and the active mutation share one mutex, making
/// validation and lease acquisition atomic across Agent and Native callers.
#[derive(Debug, Default)]
pub(super) struct MemoryForgetCoordinator {
    state: Mutex<ForgetState>,
}

#[derive(Debug)]
pub(super) struct AuthorizedForget<'a> {
    pub(super) scope: MemoryScope,
    pub(super) reservation: MemoryForgetReservation<'a>,
}

#[derive(Debug)]
pub(super) struct MemoryForgetReservation<'a> {
    coordinator: &'a MemoryForgetCoordinator,
    reservation_id: u64,
    finalized: bool,
}

impl MemoryForgetReservation<'_> {
    pub(super) fn commit(mut self, forgotten: Option<&MemoryEntry>) -> Result<(), ToolCallError> {
        self.coordinator.complete(self.reservation_id, forgotten)?;
        self.finalized = true;
        Ok(())
    }
}

impl Drop for MemoryForgetReservation<'_> {
    fn drop(&mut self) {
        if !self.finalized {
            self.coordinator.release(self.reservation_id);
        }
    }
}

impl MemoryForgetCoordinator {
    pub(super) fn record_search(
        &self,
        invocation: &MemoryToolInvocation,
        candidates: &[MemorySearchEntry],
    ) -> Result<(), ToolCallError> {
        self.record_search_at(invocation, candidates, Instant::now())
    }

    fn record_search_at(
        &self,
        invocation: &MemoryToolInvocation,
        candidates: &[MemorySearchEntry],
        now: Instant,
    ) -> Result<(), ToolCallError> {
        let mut state = self.lock_state()?;
        if state.active.as_ref().is_some_and(|active| {
            active.session_id == invocation.session_id
                && matches!(active.kind, ActiveForgetKind::AgentConfirmed { .. })
        }) {
            return Err(ToolCallError::InvalidInput(
                "memory_forget selection mutation is already in flight".to_string(),
            ));
        }
        Self::prune_expired(&mut state, now);
        if candidates.is_empty() {
            state.pending_by_session.remove(&invocation.session_id);
            return Ok(());
        }
        let selection_id = state.next_selection_id;
        state.next_selection_id = state.next_selection_id.checked_add(1).ok_or_else(|| {
            ToolCallError::InternalError("memory forget selection ID overflow".to_string())
        })?;
        state.pending_by_session.insert(
            invocation.session_id,
            PendingForgetSelection {
                selection_id,
                source_turn_id: invocation.turn_id,
                source_user_item_id: invocation.user_item_id.clone(),
                candidates: candidates
                    .iter()
                    .map(|candidate| PendingForgetCandidate {
                        entry_id: candidate.entry_id.clone(),
                        scope: candidate.scope,
                    })
                    .collect(),
                expires_at: now + PENDING_SELECTION_TTL,
            },
        );
        Ok(())
    }

    pub(super) fn remove_session(
        &self,
        session_id: devo_protocol::SessionId,
    ) -> Result<(), ToolCallError> {
        self.lock_state()?.pending_by_session.remove(&session_id);
        Ok(())
    }

    pub(super) fn authorize_agent(
        &self,
        invocation: &MemoryToolInvocation,
        user_text: &str,
        entry_id: &MemoryEntryId,
        requested_scope: MemoryScope,
    ) -> Result<AuthorizedForget<'_>, ToolCallError> {
        self.authorize_agent_at(
            invocation,
            user_text,
            entry_id,
            requested_scope,
            Instant::now(),
        )
    }

    fn authorize_agent_at(
        &self,
        invocation: &MemoryToolInvocation,
        user_text: &str,
        entry_id: &MemoryEntryId,
        requested_scope: MemoryScope,
        now: Instant,
    ) -> Result<AuthorizedForget<'_>, ToolCallError> {
        let mut state = self.lock_state()?;
        Self::reject_active(&state)?;
        Self::prune_expired(&mut state, now);
        let (scope, kind) = if strict_forget_names(
            ForgetCommandGrammar::Direct,
            user_text,
            entry_id,
        ) {
            (requested_scope, ActiveForgetKind::AgentDirect)
        } else {
            let selection = state
                .pending_by_session
                .get(&invocation.session_id)
                .ok_or_else(|| {
                    ToolCallError::InvalidInput(
                        "memory_forget requires a strict exact stable-ID command or a pending selection"
                            .to_string(),
                    )
                })?;
            if selection.source_turn_id == invocation.turn_id
                || selection.source_user_item_id == invocation.user_item_id
            {
                return Err(ToolCallError::InvalidInput(
                    "memory_forget requires a subsequent user selection after memory_search"
                        .to_string(),
                ));
            }
            let candidate = selection
                .candidates
                .iter()
                .find(|candidate| candidate.entry_id == *entry_id)
                .ok_or_else(|| {
                    ToolCallError::InvalidInput(
                        "memory_forget target is not one of the pending candidates".to_string(),
                    )
                })?;
            if !strict_forget_names(ForgetCommandGrammar::Confirmation, user_text, entry_id) {
                return Err(ToolCallError::InvalidInput(
                    "memory_forget pending selection must explicitly confirm the selected stable ID"
                        .to_string(),
                ));
            }
            (
                candidate.scope,
                ActiveForgetKind::AgentConfirmed {
                    selection_id: selection.selection_id,
                },
            )
        };
        let reservation = self.reserve(
            &mut state,
            invocation.session_id,
            kind,
            Some(entry_id.clone()),
        )?;
        Ok(AuthorizedForget { scope, reservation })
    }

    pub(super) fn authorize_native(
        &self,
        session_id: devo_protocol::SessionId,
        entry_id: Option<&MemoryEntryId>,
    ) -> Result<MemoryForgetReservation<'_>, ToolCallError> {
        let mut state = self.lock_state()?;
        Self::reject_active(&state)?;
        self.reserve(
            &mut state,
            session_id,
            ActiveForgetKind::Native,
            entry_id.cloned(),
        )
    }

    fn reserve<'a>(
        &'a self,
        state: &mut ForgetState,
        session_id: devo_protocol::SessionId,
        kind: ActiveForgetKind,
        entry_id: Option<MemoryEntryId>,
    ) -> Result<MemoryForgetReservation<'a>, ToolCallError> {
        let reservation_id = state.next_reservation_id;
        state.next_reservation_id = state.next_reservation_id.checked_add(1).ok_or_else(|| {
            ToolCallError::InternalError("memory forget reservation ID overflow".to_string())
        })?;
        state.active = Some(ActiveForgetMutation {
            reservation_id,
            session_id,
            kind,
            entry_id,
        });
        Ok(MemoryForgetReservation {
            coordinator: self,
            reservation_id,
            finalized: false,
        })
    }

    fn complete(
        &self,
        reservation_id: u64,
        forgotten: Option<&MemoryEntry>,
    ) -> Result<(), ToolCallError> {
        let mut state = self.lock_state()?;
        let active = state
            .active
            .as_ref()
            .filter(|active| active.reservation_id == reservation_id)
            .cloned()
            .ok_or_else(|| {
                ToolCallError::InternalError(
                    "memory forget reservation is no longer current".to_string(),
                )
            })?;
        if let Some(expected_entry_id) = active.entry_id.as_ref()
            && forgotten.map(|entry| &entry.entry_id) != Some(expected_entry_id)
        {
            return Err(ToolCallError::InternalError(
                "memory forget returned an unexpected entry".to_string(),
            ));
        }
        if let ActiveForgetKind::AgentConfirmed { selection_id } = active.kind
            && state
                .pending_by_session
                .get(&active.session_id)
                .is_some_and(|selection| selection.selection_id == selection_id)
        {
            state.pending_by_session.remove(&active.session_id);
        }
        if let Some(forgotten) = forgotten {
            state.pending_by_session.retain(|_, selection| {
                selection
                    .candidates
                    .retain(|candidate| candidate.entry_id != forgotten.entry_id);
                !selection.candidates.is_empty()
            });
        }
        state.active = None;
        Ok(())
    }

    fn release(&self, reservation_id: u64) {
        if let Ok(mut state) = self.state.lock()
            && state
                .active
                .as_ref()
                .is_some_and(|active| active.reservation_id == reservation_id)
        {
            state.active = None;
        }
    }

    fn reject_active(state: &ForgetState) -> Result<(), ToolCallError> {
        if state.active.is_some() {
            return Err(ToolCallError::InvalidInput(
                "memory forget mutation is already in flight".to_string(),
            ));
        }
        Ok(())
    }

    fn prune_expired(state: &mut ForgetState, now: Instant) {
        let protected = state.active.as_ref().and_then(|active| match active.kind {
            ActiveForgetKind::AgentConfirmed { selection_id } => {
                Some((active.session_id, selection_id))
            }
            ActiveForgetKind::AgentDirect | ActiveForgetKind::Native => None,
        });
        state.pending_by_session.retain(|session_id, selection| {
            selection.expires_at > now
                || protected.is_some_and(|(protected_session, protected_selection)| {
                    *session_id == protected_session
                        && selection.selection_id == protected_selection
                })
        });
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, ForgetState>, ToolCallError> {
        self.state.lock().map_err(|_| {
            ToolCallError::InternalError(
                "memory forget coordinator state is unavailable".to_string(),
            )
        })
    }
}

fn strict_forget_names(
    grammar: ForgetCommandGrammar,
    user_text: &str,
    entry_id: &MemoryEntryId,
) -> bool {
    let text = user_text.trim();
    let english_matches = |prefix: &str| {
        text.strip_suffix(entry_id.as_str())
            .is_some_and(|command| command.eq_ignore_ascii_case(prefix))
    };
    match grammar {
        ForgetCommandGrammar::Direct => {
            english_matches("forget memory entry ") || text == format!("删除记忆条目 {entry_id}")
        }
        ForgetCommandGrammar::Confirmation => {
            english_matches("confirm forget memory entry ")
                || text == format!("确认删除记忆条目 {entry_id}")
        }
    }
}

#[cfg(test)]
mod tests;
