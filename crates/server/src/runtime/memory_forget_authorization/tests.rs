use std::collections::HashMap;
use std::time::{Duration, Instant};

use devo_core::tools::{MemoryToolInvocation, ToolCallError};
use devo_protocol::native::ids::MemoryEntryId;
use devo_protocol::native::rpc_memory::{
    MemoryEntry, MemoryKind, MemoryOrigin, MemoryProvenance, MemoryScope, MemorySearchEntry,
    MemoryState,
};
use pretty_assertions::assert_eq;

use super::{
    ActiveForgetKind, ActiveForgetMutation, AuthorizedForget, ForgetCommandGrammar, ForgetState,
    MemoryForgetCoordinator, PENDING_SELECTION_TTL, PendingForgetCandidate, PendingForgetSelection,
    strict_forget_names,
};

trait AuthorizeForTest {
    fn authorize<'a>(
        &'a self,
        invocation: &MemoryToolInvocation,
        user_text: &str,
        entry_id: &MemoryEntryId,
    ) -> Result<AuthorizedForget<'a>, ToolCallError>;

    fn authorize_at<'a>(
        &'a self,
        invocation: &MemoryToolInvocation,
        user_text: &str,
        entry_id: &MemoryEntryId,
        now: Instant,
    ) -> Result<AuthorizedForget<'a>, ToolCallError>;
}

impl AuthorizeForTest for MemoryForgetCoordinator {
    fn authorize<'a>(
        &'a self,
        invocation: &MemoryToolInvocation,
        user_text: &str,
        entry_id: &MemoryEntryId,
    ) -> Result<AuthorizedForget<'a>, ToolCallError> {
        self.authorize_agent(invocation, user_text, entry_id, MemoryScope::User)
    }

    fn authorize_at<'a>(
        &'a self,
        invocation: &MemoryToolInvocation,
        user_text: &str,
        entry_id: &MemoryEntryId,
        now: Instant,
    ) -> Result<AuthorizedForget<'a>, ToolCallError> {
        self.authorize_agent_at(invocation, user_text, entry_id, MemoryScope::User, now)
    }
}

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

fn forgotten_entry(entry_id: MemoryEntryId) -> MemoryEntry {
    let timestamp = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .expect("fixed timestamp")
        .with_timezone(&chrono::Utc);
    MemoryEntry {
        entry_id,
        scope: MemoryScope::User,
        scope_id: "user".to_string(),
        kind: MemoryKind::Preference,
        normalized_key: "key".to_string(),
        body: "body".to_string(),
        origin: MemoryOrigin::ExplicitUser,
        state: MemoryState::Retired,
        created_at: timestamp,
        updated_at: timestamp,
        replacement_entry_id: None,
        provenance: Vec::<MemoryProvenance>::new(),
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
    let authorizations = MemoryForgetCoordinator::default();
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
    let authorized = authorizations
        .authorize(
            &selection,
            &format!("Confirm forget memory entry {selected_id}"),
            &selected_id,
        )
        .expect("selected candidate is authorized");
    let forgotten = forgotten_entry(selected_id.clone());
    authorized
        .reservation
        .commit(Some(&forgotten))
        .expect("successful mutation consumes selection");
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
    let authorizations = MemoryForgetCoordinator::default();
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

    let authorized = authorizations
        .authorize(
            &direct,
            &format!("Forget memory entry {direct_id}"),
            &direct_id,
        )
        .expect("direct command is authorized despite unrelated pending state");
    assert_eq!(authorized.scope, MemoryScope::User);
}

/// Trace: L2-DES-MEM-001 DD-12
/// Verifies: a direct mutation lease blocks a pending confirmation from another session.
#[test]
fn direct_inflight_blocks_pending_confirmation() {
    let authorizations = MemoryForgetCoordinator::default();
    let direct = invocation();
    let direct_id = MemoryEntryId::from("mem_direct");
    let _direct_grant = authorizations
        .authorize(
            &direct,
            &format!("Forget memory entry {direct_id}"),
            &direct_id,
        )
        .expect("direct mutation is authorized");
    let search = invocation();
    let selected_id = MemoryEntryId::from("mem_selected");
    authorizations
        .record_search(
            &search,
            &[candidate(selected_id.clone(), MemoryScope::User)],
        )
        .expect("record pending search");
    let confirmation = MemoryToolInvocation {
        turn_id: devo_protocol::TurnId::new(),
        user_item_id: devo_protocol::native::ids::ItemId::new(),
        ..search
    };

    let blocked = authorizations
        .authorize(
            &confirmation,
            &format!("Confirm forget memory entry {selected_id}"),
            &selected_id,
        )
        .expect_err("direct mutation must block pending confirmation")
        .to_string();
    assert_eq!(
        blocked,
        "invalid input: memory forget mutation is already in flight"
    );
    drop(_direct_grant);
    authorizations
        .authorize(
            &confirmation,
            &format!("Confirm forget memory entry {selected_id}"),
            &selected_id,
        )
        .expect("dropping a failed direct mutation releases the pending confirmation");
}

/// Trace: L2-DES-MEM-001 DD-12
/// Verifies: an in-flight confirmed mutation blocks a concurrent direct exact-ID mutation from another session.
#[test]
fn inflight_selection_blocks_direct_exact_id_authority() {
    let authorizations = MemoryForgetCoordinator::default();
    let search = invocation();
    let selected_id = MemoryEntryId::from("mem_pending");
    authorizations
        .record_search(
            &search,
            &[candidate(selected_id.clone(), MemoryScope::User)],
        )
        .expect("record pending search");
    let selection = MemoryToolInvocation {
        turn_id: devo_protocol::TurnId::new(),
        user_item_id: devo_protocol::native::ids::ItemId::new(),
        ..search
    };
    let _reservation = authorizations
        .authorize(
            &selection,
            &format!("Confirm forget memory entry {selected_id}"),
            &selected_id,
        )
        .expect("reserve selected candidate")
        .reservation;
    let direct = invocation();
    let direct_id = MemoryEntryId::from("mem_direct");

    assert_eq!(
        authorizations
            .authorize(
                &direct,
                &format!("Forget memory entry {direct_id}"),
                &direct_id,
            )
            .expect_err("in-flight selection must block direct mutation")
            .to_string(),
        "invalid input: memory forget mutation is already in flight"
    );
}

/// Trace: L2-DES-MEM-001 DD-12
/// Verifies: a successful deletion removes the same stable ID from every session's pending candidates.
#[test]
fn successful_forget_removes_entry_from_all_pending_selections() {
    let authorizations = MemoryForgetCoordinator::default();
    let deleted_id = MemoryEntryId::from("mem_deleted");
    let confirming_search = invocation();
    let other_search = invocation();
    authorizations
        .record_search(
            &confirming_search,
            &[candidate(deleted_id.clone(), MemoryScope::User)],
        )
        .expect("record confirming search");
    authorizations
        .record_search(
            &other_search,
            &[
                candidate(deleted_id.clone(), MemoryScope::User),
                candidate(MemoryEntryId::from("mem_other"), MemoryScope::User),
            ],
        )
        .expect("record other search");
    let confirmation = MemoryToolInvocation {
        turn_id: devo_protocol::TurnId::new(),
        user_item_id: devo_protocol::native::ids::ItemId::new(),
        ..confirming_search
    };
    let authorized = authorizations
        .authorize(
            &confirmation,
            &format!("Confirm forget memory entry {deleted_id}"),
            &deleted_id,
        )
        .expect("reserve confirmed candidate");
    let forgotten = forgotten_entry(deleted_id.clone());
    authorized
        .reservation
        .commit(Some(&forgotten))
        .expect("commit confirmed deletion");
    let other_confirmation = MemoryToolInvocation {
        turn_id: devo_protocol::TurnId::new(),
        user_item_id: devo_protocol::native::ids::ItemId::new(),
        ..other_search
    };

    assert_eq!(
        authorizations
            .authorize(
                &other_confirmation,
                &format!("Confirm forget memory entry {deleted_id}"),
                &deleted_id,
            )
            .expect_err("deleted ID must be removed from other pending selections")
            .to_string(),
        "invalid input: memory_forget target is not one of the pending candidates"
    );
}

/// Trace: L2-DES-MEM-001 DD-12
/// Verifies: an in-flight selection excludes concurrent mutation and returns to Pending when mutation does not commit.
#[test]
fn abandoned_inflight_selection_can_be_retried() {
    let authorizations = MemoryForgetCoordinator::default();
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
        "invalid input: memory forget mutation is already in flight"
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
/// Verifies: search cannot replace or prune an active confirmed selection; normal TTL cleanup resumes after cancellation.
#[test]
fn active_confirmation_protects_its_selection_from_search_and_expiry() {
    let authorizations = MemoryForgetCoordinator::default();
    let search = invocation();
    let selected_id = MemoryEntryId::from("mem_selected");
    let started_at = Instant::now();
    let source_session_id = search.session_id;
    let source_turn_id = search.turn_id;
    let source_user_item_id = search.user_item_id.clone();
    authorizations
        .record_search_at(
            &search,
            &[candidate(selected_id.clone(), MemoryScope::User)],
            started_at,
        )
        .expect("record pending search");
    let confirmation = MemoryToolInvocation {
        turn_id: devo_protocol::TurnId::new(),
        user_item_id: devo_protocol::native::ids::ItemId::new(),
        ..search
    };
    let reservation = authorizations
        .authorize_at(
            &confirmation,
            &format!("Confirm forget memory entry {selected_id}"),
            &selected_id,
            started_at,
        )
        .expect("reserve confirmed candidate");

    assert_eq!(
        authorizations
            .record_search_at(
                &confirmation,
                &[candidate(
                    MemoryEntryId::from("mem_replacement"),
                    MemoryScope::User,
                )],
                started_at + PENDING_SELECTION_TTL + Duration::from_secs(1),
            )
            .expect_err("active confirmation must block replacement search")
            .to_string(),
        "invalid input: memory_forget selection mutation is already in flight"
    );
    let other_search = invocation();
    let other_session_id = other_search.session_id;
    let other_turn_id = other_search.turn_id;
    let other_user_item_id = other_search.user_item_id.clone();
    let other_entry_id = MemoryEntryId::from("mem_other");
    authorizations
        .record_search_at(
            &other_search,
            &[candidate(other_entry_id.clone(), MemoryScope::User)],
            started_at + PENDING_SELECTION_TTL + Duration::from_secs(1),
        )
        .expect("other session search may proceed without pruning active selection");
    assert_eq!(
        *authorizations
            .lock_state()
            .expect("inspect protected selection"),
        ForgetState {
            pending_by_session: HashMap::from([
                (
                    source_session_id,
                    PendingForgetSelection {
                        selection_id: 0,
                        source_turn_id,
                        source_user_item_id,
                        candidates: vec![PendingForgetCandidate {
                            entry_id: selected_id.clone(),
                            scope: MemoryScope::User,
                        }],
                        expires_at: started_at + PENDING_SELECTION_TTL,
                    },
                ),
                (
                    other_session_id,
                    PendingForgetSelection {
                        selection_id: 1,
                        source_turn_id: other_turn_id,
                        source_user_item_id: other_user_item_id,
                        candidates: vec![PendingForgetCandidate {
                            entry_id: other_entry_id,
                            scope: MemoryScope::User,
                        }],
                        expires_at: started_at
                            + PENDING_SELECTION_TTL
                            + Duration::from_secs(1)
                            + PENDING_SELECTION_TTL,
                    },
                ),
            ]),
            active: Some(ActiveForgetMutation {
                reservation_id: 0,
                session_id: confirmation.session_id,
                kind: ActiveForgetKind::AgentConfirmed { selection_id: 0 },
                entry_id: Some(selected_id.clone()),
            }),
            next_reservation_id: 1,
            next_selection_id: 2,
        }
    );
    drop(reservation);
    assert_eq!(
        authorizations
            .authorize_at(
                &confirmation,
                &format!("Confirm forget memory entry {selected_id}"),
                &selected_id,
                started_at + PENDING_SELECTION_TTL + Duration::from_secs(1),
            )
            .expect_err("expired selection is pruned after cancellation releases its lease")
            .to_string(),
        "invalid input: memory_forget requires a strict exact stable-ID command or a pending selection"
    );
}

/// Trace: L2-DES-MEM-001 DD-12
/// Verifies: an old reservation drop or failed commit cannot clear a newer active mutation lease.
#[test]
fn stale_reservation_cannot_clear_new_active_lease() {
    let authorizations = MemoryForgetCoordinator::default();
    let first = invocation();
    let first_id = MemoryEntryId::from("mem_first");
    let stale = authorizations
        .authorize(
            &first,
            &format!("Forget memory entry {first_id}"),
            &first_id,
        )
        .expect("reserve first direct mutation")
        .reservation;
    authorizations.release(stale.reservation_id);
    let second = invocation();
    let second_id = MemoryEntryId::from("mem_second");
    let current = authorizations
        .authorize(
            &second,
            &format!("Forget memory entry {second_id}"),
            &second_id,
        )
        .expect("reserve replacement direct mutation")
        .reservation;

    assert_eq!(
        stale
            .commit(Some(&forgotten_entry(first_id)))
            .expect_err("stale commit must fail")
            .to_string(),
        "internal error: memory forget reservation is no longer current"
    );
    let third = invocation();
    let third_id = MemoryEntryId::from("mem_third");
    assert_eq!(
        authorizations
            .authorize(
                &third,
                &format!("Forget memory entry {third_id}"),
                &third_id,
            )
            .expect_err("stale reservation must not clear current mutation")
            .to_string(),
        "invalid input: memory forget mutation is already in flight"
    );
    drop(current);
}

/// Trace: L2-DES-MEM-001 DD-12
/// Verifies: expired pending state fails closed and requires a fresh search.
#[test]
fn pending_selection_expires_fail_closed() {
    let authorizations = MemoryForgetCoordinator::default();
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
    let authorizations = MemoryForgetCoordinator::default();
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

    let state = authorizations.lock_state().expect("inspect pending state");
    assert_eq!(
        *state,
        ForgetState {
            pending_by_session: HashMap::from([(
                current.session_id,
                PendingForgetSelection {
                    selection_id: 1,
                    source_turn_id: current.turn_id,
                    source_user_item_id: current.user_item_id.clone(),
                    candidates: vec![PendingForgetCandidate {
                        entry_id: MemoryEntryId::from("mem_current"),
                        scope: MemoryScope::User,
                    }],
                    expires_at: now
                        + PENDING_SELECTION_TTL
                        + Duration::from_secs(1)
                        + PENDING_SELECTION_TTL,
                },
            )]),
            active: None::<ActiveForgetMutation>,
            next_reservation_id: 0,
            next_selection_id: 2,
        }
    );
}

/// Trace: L2-DES-MEM-001 DD-12
/// Verifies: deleting a session removes its pending forget authorization state.
#[test]
fn removing_session_clears_pending_selection() {
    let authorizations = MemoryForgetCoordinator::default();
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
            .lock_state()
            .expect("inspect pending state")
            .pending_by_session
            .is_empty()
    );
}
