use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use devo_core::tools::{MemoryToolInvocation, ToolCallError};
use devo_protocol::native::ids::MemoryEntryId;
use devo_protocol::native::rpc_memory::{MemoryScope, MemorySearchEntry};

const PENDING_SELECTION_TTL: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingForgetCandidate {
    entry_id: MemoryEntryId,
    scope: MemoryScope,
}

#[derive(Debug)]
struct PendingForgetSelection {
    source_turn_id: devo_protocol::TurnId,
    source_user_item_id: devo_protocol::native::ids::ItemId,
    candidates: Vec<PendingForgetCandidate>,
    expires_at: Instant,
    status: PendingForgetStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingForgetStatus {
    Pending,
    InFlight(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ForgetCommandGrammar {
    Direct,
    Confirmation,
}

/// Fail-closed, ephemeral authorization state for root-agent memory deletion.
///
/// Search records candidates for one user turn. Mutation either names an exact
/// stable ID in a strict direct command or selects a recorded candidate from a
/// later user turn. Restarting the server drops pending state and requires a
/// fresh search.
#[derive(Debug, Default)]
pub(super) struct MemoryForgetAuthorizations {
    pending_by_session: Mutex<HashMap<devo_protocol::SessionId, PendingForgetSelection>>,
    next_reservation_id: AtomicU64,
}

#[derive(Debug)]
pub(super) enum MemoryForgetGrant<'a> {
    Direct,
    Pending {
        scope: MemoryScope,
        reservation: PendingForgetReservation<'a>,
    },
}

#[derive(Debug)]
pub(super) struct PendingForgetReservation<'a> {
    authorizations: &'a MemoryForgetAuthorizations,
    session_id: devo_protocol::SessionId,
    reservation_id: u64,
    finalized: bool,
}

impl PendingForgetReservation<'_> {
    pub(super) fn commit(mut self) -> Result<(), ToolCallError> {
        self.authorizations
            .consume(self.session_id, self.reservation_id)?;
        self.finalized = true;
        Ok(())
    }
}

impl Drop for PendingForgetReservation<'_> {
    fn drop(&mut self) {
        if !self.finalized {
            self.authorizations
                .release(self.session_id, self.reservation_id);
        }
    }
}

impl MemoryForgetAuthorizations {
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
        let mut pending = self.lock_pending()?;
        pending.retain(|_, selection| {
            selection.status != PendingForgetStatus::Pending || selection.expires_at > now
        });
        if pending
            .get(&invocation.session_id)
            .is_some_and(|selection| matches!(selection.status, PendingForgetStatus::InFlight(_)))
        {
            return Err(ToolCallError::InvalidInput(
                "memory_forget selection mutation is already in flight".to_string(),
            ));
        }
        if candidates.is_empty() {
            pending.remove(&invocation.session_id);
        } else {
            pending.insert(
                invocation.session_id,
                PendingForgetSelection {
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
                    status: PendingForgetStatus::Pending,
                },
            );
        }
        Ok(())
    }

    pub(super) fn remove_session(
        &self,
        session_id: devo_protocol::SessionId,
    ) -> Result<(), ToolCallError> {
        self.lock_pending()?.remove(&session_id);
        Ok(())
    }

    pub(super) fn authorize(
        &self,
        invocation: &MemoryToolInvocation,
        user_text: &str,
        entry_id: &MemoryEntryId,
    ) -> Result<MemoryForgetGrant<'_>, ToolCallError> {
        self.authorize_at(invocation, user_text, entry_id, Instant::now())
    }

    fn authorize_at(
        &self,
        invocation: &MemoryToolInvocation,
        user_text: &str,
        entry_id: &MemoryEntryId,
        now: Instant,
    ) -> Result<MemoryForgetGrant<'_>, ToolCallError> {
        if strict_forget_names(ForgetCommandGrammar::Direct, user_text, entry_id) {
            return Ok(MemoryForgetGrant::Direct);
        }
        let mut pending = self.lock_pending()?;
        pending.retain(|_, selection| {
            selection.status != PendingForgetStatus::Pending || selection.expires_at > now
        });
        let Some(selection) = pending.get_mut(&invocation.session_id) else {
            return Err(ToolCallError::InvalidInput(
                "memory_forget requires a strict exact stable-ID command or a pending selection"
                    .to_string(),
            ));
        };
        if selection.source_turn_id == invocation.turn_id
            || selection.source_user_item_id == invocation.user_item_id
        {
            return Err(ToolCallError::InvalidInput(
                "memory_forget requires a subsequent user selection after memory_search"
                    .to_string(),
            ));
        }
        let Some(candidate) = selection
            .candidates
            .iter()
            .find(|candidate| candidate.entry_id == *entry_id)
        else {
            return Err(ToolCallError::InvalidInput(
                "memory_forget target is not one of the pending candidates".to_string(),
            ));
        };
        let scope = candidate.scope;
        if !strict_forget_names(ForgetCommandGrammar::Confirmation, user_text, entry_id) {
            return Err(ToolCallError::InvalidInput(
                "memory_forget pending selection must explicitly confirm the selected stable ID"
                    .to_string(),
            ));
        }
        if matches!(selection.status, PendingForgetStatus::InFlight(_)) {
            return Err(ToolCallError::InvalidInput(
                "memory_forget selection is already in flight".to_string(),
            ));
        }
        let reservation_id = self.next_reservation_id.fetch_add(1, Ordering::Relaxed);
        selection.status = PendingForgetStatus::InFlight(reservation_id);
        let session_id = invocation.session_id;
        drop(pending);
        Ok(MemoryForgetGrant::Pending {
            scope,
            reservation: PendingForgetReservation {
                authorizations: self,
                session_id,
                reservation_id,
                finalized: false,
            },
        })
    }

    fn consume(
        &self,
        session_id: devo_protocol::SessionId,
        reservation_id: u64,
    ) -> Result<(), ToolCallError> {
        let mut pending = self.lock_pending()?;
        let is_current = pending.get(&session_id).is_some_and(|selection| {
            selection.status == PendingForgetStatus::InFlight(reservation_id)
        });
        if !is_current {
            return Err(ToolCallError::InternalError(
                "memory forget reservation is no longer current".to_string(),
            ));
        }
        pending.remove(&session_id);
        Ok(())
    }

    fn release(&self, session_id: devo_protocol::SessionId, reservation_id: u64) {
        if let Ok(mut pending) = self.pending_by_session.lock()
            && let Some(selection) = pending.get_mut(&session_id)
            && selection.status == PendingForgetStatus::InFlight(reservation_id)
        {
            selection.status = PendingForgetStatus::Pending;
        }
    }

    fn lock_pending(
        &self,
    ) -> Result<
        std::sync::MutexGuard<'_, HashMap<devo_protocol::SessionId, PendingForgetSelection>>,
        ToolCallError,
    > {
        self.pending_by_session.lock().map_err(|_| {
            ToolCallError::InternalError(
                "memory forget authorization state is unavailable".to_string(),
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
mod tests {
    use std::time::{Duration, Instant};

    use devo_core::tools::MemoryToolInvocation;
    use devo_protocol::native::ids::MemoryEntryId;
    use devo_protocol::native::rpc_memory::{
        MemoryKind, MemoryScope, MemorySearchEntry, MemoryState,
    };
    use pretty_assertions::assert_eq;

    use super::{
        ForgetCommandGrammar, MemoryForgetAuthorizations, MemoryForgetGrant, PENDING_SELECTION_TTL,
        strict_forget_names,
    };

    fn invocation() -> MemoryToolInvocation {
        MemoryToolInvocation {
            session_id: devo_protocol::SessionId::new(),
            turn_id: devo_protocol::TurnId::new(),
            user_item_id: devo_protocol::native::ids::ItemId::new(),
        }
    }

    fn candidate(entry_id: MemoryEntryId, scope: MemoryScope) -> MemorySearchEntry {
        MemorySearchEntry {
            entry_id,
            scope,
            kind: MemoryKind::Preference,
            state: MemoryState::Active,
            summary: "summary".to_string(),
        }
    }

    /// Trace: L2-DES-MEM-001 DD-12
    /// Verifies: direct and confirmation commands anchor their grammar while preserving byte-exact ID casing.
    #[test]
    fn direct_exact_id_grammar_is_closed_over_task_rewrites() {
        let entry_id = MemoryEntryId::from("mem_test");
        assert!(strict_forget_names(
            ForgetCommandGrammar::Direct,
            "Forget memory entry mem_test",
            &entry_id
        ));
        assert!(strict_forget_names(
            ForgetCommandGrammar::Direct,
            "删除记忆条目 mem_test",
            &entry_id
        ));
        assert!(strict_forget_names(
            ForgetCommandGrammar::Direct,
            "FORGET MEMORY ENTRY mem_test",
            &entry_id
        ));
        assert!(!strict_forget_names(
            ForgetCommandGrammar::Direct,
            "Forget memory entry MEM_TEST",
            &entry_id
        ));
        assert!(strict_forget_names(
            ForgetCommandGrammar::Confirmation,
            "CONFIRM FORGET MEMORY ENTRY mem_test",
            &entry_id
        ));
        assert!(!strict_forget_names(
            ForgetCommandGrammar::Confirmation,
            "Confirm forget memory entry MEM_TEST",
            &entry_id
        ));

        for prefix in [
            "Please forget that I use tests",
            "Forget about memory safety",
            "请忘记我在写测试",
            "请删除我不需要的文件",
        ] {
            for separator in [" and ", "; ", ". ", "\n", "，", "；"] {
                for suffix in ["implement docs", "then continue", "然后实现文档"] {
                    let text = format!("{prefix}{separator}{suffix}");
                    assert!(
                        !strict_forget_names(ForgetCommandGrammar::Direct, &text, &entry_id),
                        "{text}"
                    );
                    let uppercase = text.to_ascii_uppercase();
                    assert!(
                        !strict_forget_names(ForgetCommandGrammar::Direct, &uppercase, &entry_id),
                        "{uppercase}"
                    );
                }
            }
        }
        for text in [
            "Do not forget memory entry mem_test",
            "Please explain forget memory entry mem_test",
            "Forget memory entry mem_test and implement docs",
            "删除记忆条目 mem_test，然后实现文档",
        ] {
            assert!(
                !strict_forget_names(ForgetCommandGrammar::Direct, text, &entry_id),
                "{text}"
            );
        }
    }

    /// Trace: L2-DES-MEM-001 DD-12
    /// Verifies: pending authorization rejects same-turn and outside-candidate mutations, then accepts a later exact selection once.
    #[test]
    fn pending_selection_is_turn_bound_candidate_bound_and_single_use() {
        let authorizations = MemoryForgetAuthorizations::default();
        let search = invocation();
        let selected_id = MemoryEntryId::from("mem_selected");
        let outside_id = MemoryEntryId::from("mem_outside");
        authorizations
            .record_search(
                &search,
                &[candidate(selected_id.clone(), MemoryScope::User)],
            )
            .expect("record pending search");

        assert_eq!(
            authorizations
                .authorize(
                    &search,
                    &format!("Confirm forget memory entry {selected_id}"),
                    &selected_id,
                )
                .expect_err("same-turn forget must fail")
                .to_string(),
            "invalid input: memory_forget requires a subsequent user selection after memory_search"
        );
        let selection = MemoryToolInvocation {
            turn_id: devo_protocol::TurnId::new(),
            user_item_id: devo_protocol::native::ids::ItemId::new(),
            ..search
        };
        assert_eq!(
            authorizations
                .authorize(
                    &selection,
                    &format!("Confirm forget memory entry {outside_id}"),
                    &outside_id,
                )
                .expect_err("outside candidate must fail")
                .to_string(),
            "invalid input: memory_forget target is not one of the pending candidates"
        );
        assert_eq!(
            authorizations
                .authorize(&selection, selected_id.as_str(), &selected_id)
                .expect_err("bare candidate ID must not authorize mutation")
                .to_string(),
            "invalid input: memory_forget pending selection must explicitly confirm the selected stable ID"
        );
        let grant = authorizations
            .authorize(
                &selection,
                &format!("Confirm forget memory entry {selected_id}"),
                &selected_id,
            )
            .expect("selected candidate is authorized");
        match grant {
            MemoryForgetGrant::Direct => panic!("pending selection returned a direct grant"),
            MemoryForgetGrant::Pending { reservation, .. } => reservation
                .commit()
                .expect("successful mutation consumes selection"),
        }
        assert_eq!(
            authorizations
                .authorize(
                    &selection,
                    &format!("Confirm forget memory entry {selected_id}"),
                    &selected_id,
                )
                .expect_err("consumed selection must fail")
                .to_string(),
            "invalid input: memory_forget requires a strict exact stable-ID command or a pending selection"
        );
    }

    /// Trace: L2-DES-MEM-001 DD-12
    /// Verifies: an unrelated Pending selection never shadows an independently authorized direct exact-ID command.
    #[test]
    fn pending_selection_does_not_shadow_direct_exact_id_authority() {
        let authorizations = MemoryForgetAuthorizations::default();
        let search = invocation();
        authorizations
            .record_search(
                &search,
                &[candidate(
                    MemoryEntryId::from("mem_pending"),
                    MemoryScope::User,
                )],
            )
            .expect("record pending search");
        let direct = MemoryToolInvocation {
            turn_id: devo_protocol::TurnId::new(),
            user_item_id: devo_protocol::native::ids::ItemId::new(),
            ..search
        };
        let direct_id = MemoryEntryId::from("mem_direct");

        assert!(matches!(
            authorizations.authorize(
                &direct,
                &format!("Forget memory entry {direct_id}"),
                &direct_id,
            ),
            Ok(MemoryForgetGrant::Direct)
        ));
    }

    /// Trace: L2-DES-MEM-001 DD-12
    /// Verifies: an in-flight selection excludes concurrent mutation and returns to Pending when mutation does not commit.
    #[test]
    fn abandoned_inflight_selection_can_be_retried() {
        let authorizations = MemoryForgetAuthorizations::default();
        let search = invocation();
        let entry_id = MemoryEntryId::from("mem_retry");
        authorizations
            .record_search(&search, &[candidate(entry_id.clone(), MemoryScope::User)])
            .expect("record pending search");
        let selection = MemoryToolInvocation {
            turn_id: devo_protocol::TurnId::new(),
            user_item_id: devo_protocol::native::ids::ItemId::new(),
            ..search
        };

        let reservation = authorizations
            .authorize(
                &selection,
                &format!("Confirm forget memory entry {entry_id}"),
                &entry_id,
            )
            .expect("reserve selected candidate");
        assert_eq!(
            authorizations
                .authorize(
                    &selection,
                    &format!("Confirm forget memory entry {entry_id}"),
                    &entry_id,
                )
                .expect_err("concurrent selection must fail")
                .to_string(),
            "invalid input: memory_forget selection is already in flight"
        );
        drop(reservation);
        authorizations
            .authorize(
                &selection,
                &format!("Confirm forget memory entry {entry_id}"),
                &entry_id,
            )
            .expect("abandoned selection returns to pending");
    }

    /// Trace: L2-DES-MEM-001 DD-12
    /// Verifies: expired pending state fails closed and requires a fresh search.
    #[test]
    fn pending_selection_expires_fail_closed() {
        let authorizations = MemoryForgetAuthorizations::default();
        let search = invocation();
        let entry_id = MemoryEntryId::from("mem_expired");
        authorizations
            .record_search(&search, &[candidate(entry_id.clone(), MemoryScope::User)])
            .expect("record pending search");
        let selection = MemoryToolInvocation {
            turn_id: devo_protocol::TurnId::new(),
            user_item_id: devo_protocol::native::ids::ItemId::new(),
            ..search
        };

        assert_eq!(
            authorizations
                .authorize_at(
                    &selection,
                    &format!("Confirm forget memory entry {entry_id}"),
                    &entry_id,
                    Instant::now() + PENDING_SELECTION_TTL + Duration::from_secs(1),
                )
                .expect_err("expired selection must fail")
                .to_string(),
            "invalid input: memory_forget requires a strict exact stable-ID command or a pending selection"
        );
    }

    /// Trace: L2-DES-MEM-001 DD-12
    /// Verifies: recording a later search removes expired pending selections from other sessions.
    #[test]
    fn recording_search_prunes_expired_pending_selections() {
        let authorizations = MemoryForgetAuthorizations::default();
        let expired = invocation();
        let current = invocation();
        let now = Instant::now();
        authorizations
            .record_search_at(
                &expired,
                &[candidate(
                    MemoryEntryId::from("mem_expired"),
                    MemoryScope::User,
                )],
                now,
            )
            .expect("record expiring search");
        authorizations
            .record_search_at(
                &current,
                &[candidate(
                    MemoryEntryId::from("mem_current"),
                    MemoryScope::User,
                )],
                now + PENDING_SELECTION_TTL + Duration::from_secs(1),
            )
            .expect("record current search");

        let pending = authorizations
            .lock_pending()
            .expect("inspect pending state");
        assert_eq!(pending.len(), 1);
        assert!(pending.contains_key(&current.session_id));
    }

    /// Trace: L2-DES-MEM-001 DD-12
    /// Verifies: deleting a session removes its pending forget authorization state.
    #[test]
    fn removing_session_clears_pending_selection() {
        let authorizations = MemoryForgetAuthorizations::default();
        let search = invocation();
        authorizations
            .record_search(
                &search,
                &[candidate(
                    MemoryEntryId::from("mem_deleted_session"),
                    MemoryScope::User,
                )],
            )
            .expect("record pending search");

        authorizations
            .remove_session(search.session_id)
            .expect("remove session authorization");

        assert!(
            authorizations
                .lock_pending()
                .expect("inspect pending state")
                .is_empty()
        );
    }
}
