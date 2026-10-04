use std::path::Path;

use chrono::{DateTime, Duration, Utc};
use devo_protocol::native::ids::{ItemId, SessionId, TurnId};
use devo_protocol::native::rpc_memory::{
    MemoryEntry, MemoryKind, MemoryOrigin, MemoryScope, MemoryState,
};
use devo_protocol::native::session::MemorySetting;
use pretty_assertions::assert_eq;

use super::extraction::ExtractionCandidate;
use super::runtime_test_support::remember_request;
use super::source::{ExtractableSource, SourceMessage};
use super::{
    ListMemoryRequest, MemoryCommand, MemoryCommandResult, MemoryRuntime, PrepareMemoryRequest,
    SearchMemoryRequest,
};

fn open_runtime(root: &Path) -> MemoryRuntime {
    MemoryRuntime::open_with_clock(
        root.to_path_buf(),
        devo_core::MemoryConfig {
            enabled: true,
            ..Default::default()
        },
        std::sync::Arc::new(epoch),
    )
    .unwrap()
}

fn epoch() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
}

fn contribute(
    runtime: &MemoryRuntime,
    body: &str,
    key: &str,
    now: DateTime<Utc>,
) -> ExtractableSource {
    let turn_id = TurnId::new();
    let source = ExtractableSource {
        session_id: SessionId::from_legacy_uuid(uuid::Uuid::new_v4()),
        workspace_root: Default::default(),
        session_contribution: MemorySetting::On,
        observed_at: now - Duration::hours(7),
        watermark: "finished".into(),
        messages: vec![SourceMessage {
            turn_id: turn_id.clone(),
            item_id: ItemId::new(),
            role: "user".into(),
            observed_at: now - Duration::hours(7),
            text: body.into(),
        }],
    };
    let candidate = ExtractionCandidate {
        scope: MemoryScope::User,
        kind: MemoryKind::Preference,
        key: key.into(),
        body: body.into(),
        evidence: vec![turn_id],
    };
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime
        .commit_extraction(&claim, &source, &[candidate], now)
        .unwrap();
    source
}

fn entries(runtime: &MemoryRuntime) -> Vec<MemoryEntry> {
    runtime.list(ListMemoryRequest::default()).unwrap().data
}

async fn remember(runtime: &MemoryRuntime, text: &str) -> MemoryEntry {
    let MemoryCommandResult::Remember(entry) = runtime
        .execute_command(MemoryCommand::Remember(remember_request(text)))
        .await
        .unwrap()
    else {
        panic!("remember result")
    };
    entry
}

fn search(
    runtime: &MemoryRuntime,
    state: Option<MemoryState>,
) -> Vec<devo_protocol::native::rpc_memory::MemorySearchEntry> {
    runtime
        .search(SearchMemoryRequest {
            query: "prefer".into(),
            scope: MemoryScope::User,
            kind: None,
            state,
            workspace_root: Default::default(),
        })
        .unwrap()
        .data
}

async fn recall(
    runtime: &MemoryRuntime,
    workspace: &Path,
) -> Vec<devo_protocol::native::rpc_memory::MemoryRecallEntry> {
    runtime
        .prepare_turn(PrepareMemoryRequest {
            query: "prefer tabs spaces".into(),
            workspace_root: workspace.to_path_buf(),
            session_recall: MemorySetting::On,
        })
        .await
        .unwrap()
        .entries
}

/// Trace: L2-DES-MEM-001 Rev 4 Entry Lifecycle and Retention / DD-10.
/// Verifies: the exact 90-day boundary withholds inferred entries from automatic and default on-demand recall.
#[tokio::test]
async fn inferred_staleness_boundary_withholds_recall_and_remains_inspectable() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(&runtime, "I prefer tabs.", "indentation", epoch());
    let original = entries(&runtime).remove(0);
    runtime
        .prune_expired(epoch() + Duration::days(90) - Duration::milliseconds(1))
        .unwrap();
    assert_eq!(entries(&runtime), vec![original.clone()]);
    runtime.prune_expired(epoch() + Duration::days(90)).unwrap();
    assert_eq!(
        entries(&runtime),
        vec![MemoryEntry {
            state: MemoryState::Stale,
            ..original.clone()
        }]
    );
    assert_eq!(search(&runtime, /*state*/ None), vec![]);
    assert_eq!(
        search(&runtime, Some(MemoryState::Stale)),
        vec![devo_protocol::native::rpc_memory::MemorySearchEntry {
            entry_id: original.entry_id,
            scope: MemoryScope::User,
            kind: MemoryKind::Preference,
            state: MemoryState::Stale,
            summary: "I prefer tabs.".into(),
        }]
    );
    assert_eq!(recall(&runtime, root.path()).await, vec![]);
    assert!(
        std::fs::read_to_string(root.path().join("user/MEMORY.md"))
            .unwrap()
            .contains("state: stale")
    );
}

/// Trace: L2-DES-MEM-001 Rev 4 Entry Lifecycle and Retention.
/// Verifies: a later recall timestamp postpones expiry without changing the accepted body or evidence.
#[test]
fn last_recall_extends_inferred_lifetime() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(&runtime, "I prefer tabs.", "indentation", epoch());
    let original = entries(&runtime).remove(0);
    runtime
        .connection
        .lock()
        .unwrap()
        .execute(
            "UPDATE memory_entries SET last_recalled_at = ?1 WHERE entry_id = ?2",
            rusqlite::params![
                (epoch() + Duration::days(89)).to_rfc3339(),
                original.entry_id.as_str()
            ],
        )
        .unwrap();
    runtime.prune_expired(epoch() + Duration::days(90)).unwrap();
    assert_eq!(entries(&runtime), vec![original.clone()]);
    runtime
        .prune_expired(epoch() + Duration::days(179))
        .unwrap();
    assert_eq!(
        entries(&runtime),
        vec![MemoryEntry {
            state: MemoryState::Stale,
            ..original
        }]
    );
}

/// Trace: L2-DES-MEM-001 Rev 4 Entry Lifecycle and Retention / DD-8.
/// Verifies: fresh equivalent verification reactivates stale inference while retaining canonical identity and evidence.
#[test]
fn fresh_equivalent_evidence_reactivates_stale_memory() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(&runtime, "I prefer tabs.", "indentation", epoch());
    let original = entries(&runtime).remove(0);
    runtime
        .connection
        .lock()
        .unwrap()
        .execute(
            "UPDATE memory_entries SET state = 'stale' WHERE entry_id = ?1",
            [original.entry_id.as_str()],
        )
        .unwrap();
    let source = contribute(
        &runtime,
        "(i prefer tabs)!",
        "formatting",
        epoch() + Duration::days(91),
    );
    let actual = entries(&runtime).remove(0);
    let mut provenance = original.provenance.clone();
    provenance.push(devo_protocol::native::rpc_memory::MemoryProvenance {
        source_session_id: Some(source.session_id.to_string()),
        source_turn_id: Some(source.messages[0].turn_id.to_string()),
        source_user_item_id: Some(source.messages[0].item_id.clone()),
    });
    assert_eq!(
        actual,
        MemoryEntry {
            state: MemoryState::Active,
            updated_at: epoch() + Duration::days(91),
            provenance,
            ..original.clone()
        }
    );
    assert_eq!(
        search(&runtime, /*state*/ None),
        vec![devo_protocol::native::rpc_memory::MemorySearchEntry {
            entry_id: original.entry_id,
            scope: MemoryScope::User,
            kind: MemoryKind::Preference,
            state: MemoryState::Active,
            summary: "I prefer tabs.".into(),
        }]
    );
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8.
/// Verifies: conflicts retain the opposing body in the human-readable projection, not only private SQLite candidate rows.
#[test]
fn conflicting_claim_is_inspectable_in_projection() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(&runtime, "I prefer tabs.", "indentation", epoch());
    let original = entries(&runtime).remove(0);
    contribute(
        &runtime,
        "I prefer spaces.",
        "indentation",
        epoch() + Duration::days(1),
    );
    assert_eq!(
        entries(&runtime),
        vec![MemoryEntry {
            state: MemoryState::Conflicted,
            ..original
        }]
    );
    assert_eq!(search(&runtime, /*state*/ None), vec![]);
    let projection = std::fs::read_to_string(root.path().join("user/MEMORY.md")).unwrap();
    assert!(
        projection.contains("I prefer spaces."),
        "opposing claim must be inspectable: {projection}"
    );
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8 / Entry Lifecycle and Retention.
/// Verifies: explicit selection replaces the other established claim, preserves lineage, and cannot be undone by extraction.
#[tokio::test]
async fn explicit_resolution_preserves_replacement_lineage_and_authority() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(&runtime, "I prefer tabs.", "indentation", epoch());
    let inferred = entries(&runtime).remove(0);
    contribute(
        &runtime,
        "I prefer spaces.",
        "indentation",
        epoch() + Duration::days(1),
    );
    let tabs = remember(&runtime, "I prefer tabs.").await;
    let spaces = remember(&runtime, "I prefer spaces.").await;
    assert_eq!(
        runtime.entry_by_id(&tabs.entry_id).unwrap(),
        Some(MemoryEntry {
            state: MemoryState::Retired,
            replacement_entry_id: Some(spaces.entry_id.clone()),
            ..tabs.clone()
        })
    );
    let source = remember_request("I prefer tabs.").source;
    let provenance = devo_protocol::native::rpc_memory::MemoryProvenance {
        source_session_id: Some(source.session_id.to_string()),
        source_turn_id: source.turn_id.map(|id| id.to_string()),
        source_user_item_id: source.user_item_id,
    };
    let mut tabs_provenance = inferred.provenance.clone();
    tabs_provenance.push(provenance.clone());
    assert_eq!(
        tabs,
        MemoryEntry {
            origin: MemoryOrigin::ExplicitUser,
            provenance: tabs_provenance,
            ..inferred
        }
    );
    assert_eq!(
        spaces,
        MemoryEntry {
            entry_id: spaces.entry_id.clone(),
            scope: MemoryScope::User,
            scope_id: "user".into(),
            kind: MemoryKind::Preference,
            normalized_key: "i prefer spaces".into(),
            body: "I prefer spaces.".into(),
            origin: MemoryOrigin::ExplicitUser,
            state: MemoryState::Active,
            created_at: epoch(),
            updated_at: epoch(),
            replacement_entry_id: None,
            provenance: vec![provenance],
        }
    );
    contribute(
        &runtime,
        "I prefer tabs.",
        "another model label",
        epoch() + Duration::days(2),
    );
    assert_eq!(
        runtime.entry_by_id(&spaces.entry_id).unwrap(),
        Some(spaces.clone())
    );
    assert_eq!(
        search(&runtime, /*state*/ None),
        vec![devo_protocol::native::rpc_memory::MemorySearchEntry {
            entry_id: spaces.entry_id.clone(),
            scope: MemoryScope::User,
            kind: MemoryKind::Preference,
            state: MemoryState::Active,
            summary: "I prefer spaces.".into(),
        }]
    );
    let tabs_again = remember(&runtime, "I prefer tabs.").await;
    assert_eq!(tabs_again, tabs);
    assert_eq!(
        runtime.entry_by_id(&spaces.entry_id).unwrap(),
        Some(MemoryEntry {
            state: MemoryState::Retired,
            replacement_entry_id: Some(tabs_again.entry_id.clone()),
            ..spaces
        })
    );
    runtime
        .prune_expired(epoch() + Duration::days(500))
        .unwrap();
    assert_eq!(
        runtime.entry_by_id(&tabs_again.entry_id).unwrap(),
        Some(tabs_again)
    );
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8.
/// Verifies: choosing the unbound opposing candidate retires the inferred canonical entry rather than leaving an unresolved conflict.
#[tokio::test]
async fn explicit_opposing_claim_resolves_conflicted_key() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(&runtime, "I prefer tabs.", "indentation", epoch());
    let inferred = entries(&runtime).remove(0);
    contribute(
        &runtime,
        "I prefer spaces.",
        "indentation",
        epoch() + Duration::days(1),
    );
    let explicit = remember(&runtime, "I prefer spaces.").await;
    assert_eq!(
        runtime.entry_by_id(&inferred.entry_id).unwrap(),
        Some(MemoryEntry {
            state: MemoryState::Retired,
            replacement_entry_id: Some(explicit.entry_id.clone()),
            ..inferred
        })
    );
    assert_eq!(
        search(&runtime, /*state*/ None),
        vec![devo_protocol::native::rpc_memory::MemorySearchEntry {
            entry_id: explicit.entry_id,
            scope: MemoryScope::User,
            kind: MemoryKind::Preference,
            state: MemoryState::Active,
            summary: "I prefer spaces.".into(),
        }]
    );
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8.
/// Verifies: equivalent inferred evidence never refreshes the accepted explicit revision timestamp.
#[tokio::test]
async fn inferred_evidence_does_not_rewrite_explicit_revision() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let original = remember(&runtime, "I prefer tabs.").await;
    let source = contribute(
        &runtime,
        "I prefer tabs.",
        "indentation",
        epoch() + Duration::days(1),
    );
    let actual = entries(&runtime).remove(0);
    let mut provenance = original.provenance.clone();
    provenance.push(devo_protocol::native::rpc_memory::MemoryProvenance {
        source_session_id: Some(source.session_id.to_string()),
        source_turn_id: Some(source.messages[0].turn_id.to_string()),
        source_user_item_id: Some(source.messages[0].item_id.clone()),
    });
    // Evidence ordering is chronological: the inferred fixture is later than the explicit write.
    assert_eq!(
        actual,
        MemoryEntry {
            provenance,
            ..original
        }
    );
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8.
/// Verifies: failure during an explicit replacement rolls back both entries and the lexical index.
#[tokio::test]
async fn explicit_resolution_is_transactional() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(&runtime, "I prefer tabs.", "indentation", epoch());
    contribute(
        &runtime,
        "I prefer spaces.",
        "indentation",
        epoch() + Duration::days(1),
    );
    let before = entries(&runtime);
    runtime
        .connection
        .lock()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_explicit_evidence BEFORE INSERT ON memory_evidence
         WHEN NEW.source_user_item_id IS NOT NULL BEGIN SELECT RAISE(ABORT, 'injected'); END;",
        )
        .unwrap();
    assert!(
        runtime
            .execute_command(MemoryCommand::Remember(remember_request(
                "I prefer spaces."
            )))
            .await
            .is_err()
    );
    assert_eq!(entries(&runtime), before);
    assert_eq!(search(&runtime, /*state*/ None), vec![]);
    runtime
        .connection
        .lock()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_explicit_evidence")
        .unwrap();
    let winner = remember(&runtime, "I prefer spaces.").await;
    assert_eq!(
        runtime.entry_by_id(&before[0].entry_id).unwrap(),
        Some(MemoryEntry {
            state: MemoryState::Retired,
            replacement_entry_id: Some(winner.entry_id),
            ..before[0].clone()
        })
    );
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8.
/// Verifies: explicit selection only retires claims from its own scope, not a shared proposal label elsewhere.
#[tokio::test]
async fn explicit_resolution_is_scope_isolated() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(&runtime, "I prefer tabs.", "indentation", epoch());
    contribute(
        &runtime,
        "I prefer spaces.",
        "indentation",
        epoch() + Duration::days(1),
    );
    let before = entries(&runtime);
    let mut request = remember_request("I prefer spaces.");
    request.scope = MemoryScope::Project;
    request.source.workspace_root = root.path().to_path_buf();
    let MemoryCommandResult::Remember(selected) = runtime
        .execute_command(MemoryCommand::Remember(request))
        .await
        .unwrap()
    else {
        panic!("remember result")
    };
    assert_eq!(entries(&runtime), before);
    let source = remember_request("I prefer spaces.").source;
    assert_eq!(
        selected,
        MemoryEntry {
            entry_id: selected.entry_id.clone(),
            scope: MemoryScope::Project,
            scope_id: selected.scope_id.clone(),
            kind: MemoryKind::Preference,
            normalized_key: "i prefer spaces".into(),
            body: "I prefer spaces.".into(),
            origin: MemoryOrigin::ExplicitUser,
            state: MemoryState::Active,
            created_at: epoch(),
            updated_at: epoch(),
            replacement_entry_id: None,
            provenance: vec![devo_protocol::native::rpc_memory::MemoryProvenance {
                source_session_id: Some(source.session_id.to_string()),
                source_turn_id: source.turn_id.map(|id| id.to_string()),
                source_user_item_id: source.user_item_id,
            }],
        }
    );
    assert_eq!(search(&runtime, /*state*/ None), vec![]);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8 / Storage Model.
/// Verifies: opposing claims remain inspectable after short-lived candidate details are pruned.
#[test]
fn conflict_inspection_survives_candidate_retention() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(&runtime, "I prefer tabs.", "indentation", epoch());
    contribute(
        &runtime,
        "I prefer spaces.",
        "indentation",
        epoch() + Duration::days(1),
    );
    runtime.prune_expired(epoch() + Duration::days(40)).unwrap();
    let projection = std::fs::read_to_string(root.path().join("user/MEMORY.md")).unwrap();
    assert!(
        projection.contains("i prefer spaces"),
        "durable competing claim must remain inspectable: {projection}"
    );
    assert!(!projection.contains("I prefer spaces."));
    assert_eq!(search(&runtime, /*state*/ None), vec![]);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8.
/// Verifies: a superseded inferred entry cannot be reactivated when its explicit replacement is later retired.
#[tokio::test]
async fn superseded_inference_cannot_reactivate_after_authority_retires() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(&runtime, "I prefer tabs.", "indentation", epoch());
    let original = entries(&runtime).remove(0);
    contribute(
        &runtime,
        "I prefer spaces.",
        "indentation",
        epoch() + Duration::days(1),
    );
    let selected = remember(&runtime, "I prefer spaces.").await;
    // Build the superseded fixture explicitly so admission is tested independently of resolution.
    let selected_id = &selected.entry_id;
    let original_id = &original.entry_id;
    runtime.connection.lock().unwrap().execute_batch(&format!(
        "UPDATE memory_entries SET state = 'retired', replacement_entry_id = '{selected_id}' WHERE entry_id = '{original_id}';
         UPDATE memory_entries SET state = 'retired' WHERE entry_id = '{selected_id}';
         DELETE FROM memory_entries_fts;"
    )).unwrap();
    let before = entries(&runtime);
    contribute(
        &runtime,
        "I prefer tabs.",
        "another model label",
        epoch() + Duration::days(2),
    );
    assert_eq!(entries(&runtime), before);
    assert_eq!(search(&runtime, /*state*/ None), vec![]);
}
#[path = "lifecycle_recall_tests.rs"]
mod recall_tests;
