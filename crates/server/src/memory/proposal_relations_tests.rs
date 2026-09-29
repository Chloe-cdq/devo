use std::path::Path;

use chrono::{Duration, Utc};
use devo_protocol::native::ids::{ItemId, SessionId, TurnId};
use devo_protocol::native::rpc_memory::{
    MemoryEntry, MemoryOrigin, MemoryProvenance, MemoryScope, MemoryState,
};
use devo_protocol::native::session::MemorySetting;
use pretty_assertions::assert_eq;

use super::extraction::ExtractionCandidate;
use super::runtime_test_support::{forget_request, open_runtime, prepare_forget, remember_request};
use super::source::{ExtractableSource, SourceMessage};
use super::{
    ListMemoryRequest, MemoryCommand, MemoryCommandResult, MemoryForgetSelector, MemoryRuntime,
};

enum ContributionScope<'a> {
    User,
    Project(&'a Path),
}

fn contribute(
    runtime: &MemoryRuntime,
    body: &str,
    key: &str,
    scope: ContributionScope<'_>,
) -> ExtractableSource {
    let (scope, workspace_root) = match scope {
        ContributionScope::User => (MemoryScope::User, Default::default()),
        ContributionScope::Project(root) => (MemoryScope::Project, root.to_path_buf()),
    };
    let now = Utc::now();
    let turn_id = TurnId::new();
    let source = ExtractableSource {
        session_id: SessionId::new(),
        workspace_root,
        session_contribution: MemorySetting::On,
        observed_at: now - Duration::hours(7),
        watermark: "finished-source".into(),
        messages: vec![SourceMessage {
            turn_id: turn_id.clone(),
            item_id: ItemId::new(),
            role: "user".into(),
            observed_at: now - Duration::hours(7),
            text: body.into(),
        }],
    };
    let candidate = ExtractionCandidate {
        scope,
        kind: devo_protocol::native::rpc_memory::MemoryKind::Preference,
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

fn install_v5_active_competitor(runtime: &MemoryRuntime) {
    runtime.connection.lock().unwrap().execute_batch(
        "INSERT INTO memory_entries(entry_id, scope_type, scope_id, kind, normalized_key, body, origin, state, created_at, updated_at)
         VALUES('historical-opposing', 'user', 'user', 'preference', 'i prefer spaces', 'I prefer spaces.', 'inferred_session', 'active', '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z');
         INSERT INTO memory_entries_fts(entry_id, normalized_key, body) VALUES('historical-opposing', 'i prefer spaces', 'I prefer spaces.');
         INSERT INTO memory_evidence VALUES('historical-evidence', 'historical-opposing', 'historical-source', 'historical-turn', NULL, '2026-09-01T00:00:00Z', 'historical-watermark');
         INSERT INTO memory_candidates VALUES('historical-candidate', 'user', 'user', 'preference', 'indentation', 'I prefer spaces.', 'inferred_session', 'historical-source', 'accepted', '2026-10-01T00:00:00Z', '2026-09-01T00:00:00Z');
         DROP TABLE IF EXISTS memory_proposal_claims;
         UPDATE memory_schema_meta SET value = '5' WHERE key = 'schema_version';"
    ).unwrap();
}

async fn remember(runtime: &MemoryRuntime, body: &str) -> MemoryEntry {
    match runtime
        .execute_command(MemoryCommand::Remember(remember_request(body)))
        .await
        .unwrap()
    {
        MemoryCommandResult::Remember(entry) => entry,
        MemoryCommandResult::Forget(_)
        | MemoryCommandResult::PreparedForget(_)
        | MemoryCommandResult::List(_)
        | MemoryCommandResult::Status(_)
        | MemoryCommandResult::Search(_) => panic!("remember result"),
    }
}

fn assert_recallable(runtime: &MemoryRuntime, expected: &[MemoryEntry]) {
    let mut actual = runtime
        .list_recallable(ListMemoryRequest {
            state: Some(MemoryState::Active),
            ..ListMemoryRequest::default()
        })
        .unwrap()
        .data;
    actual.sort_by(|a, b| a.entry_id.cmp(&b.entry_id));
    let mut expected = expected.to_vec();
    expected.sort_by(|a, b| a.entry_id.cmp(&b.entry_id));
    assert_eq!(actual, expected);
    let connection = runtime.connection.lock().unwrap();
    let indexed = connection
        .prepare("SELECT entry_id, body FROM memory_entries_fts ORDER BY entry_id")
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        indexed,
        expected
            .iter()
            .map(|entry| (entry.entry_id.to_string(), entry.body.clone()))
            .collect::<Vec<_>>()
    );
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8.
/// Verifies: changing an equivalent explicit display never loses the prior proposal's authority.
#[tokio::test]
async fn equivalent_explicit_display_preserves_proposal_authority() {
    for (display, normalized_display) in [
        ("i prefer tabs", "i prefer tabs"),
        ("(I prefer tabs).", "(I prefer tabs)."),
        ("\"I prefer tabs\".", "\"I prefer tabs\"."),
        ("  I  prefer   tabs!  ", "I prefer tabs!"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let runtime = open_runtime(root.path());
        contribute(
            &runtime,
            "I prefer tabs.",
            "indentation",
            ContributionScope::User,
        );
        let inferred = runtime
            .list(ListMemoryRequest::default())
            .unwrap()
            .data
            .remove(0);
        let explicit = remember(&runtime, display).await;
        let request = remember_request(display);
        let mut provenance = inferred.provenance.clone();
        provenance.push(MemoryProvenance {
            source_session_id: Some(request.source.session_id.to_string()),
            source_turn_id: request.source.turn_id.map(|turn| turn.to_string()),
            source_user_item_id: request.source.user_item_id,
        });
        assert_eq!(
            explicit,
            MemoryEntry {
                body: normalized_display.to_string(),
                origin: MemoryOrigin::ExplicitUser,
                state: MemoryState::Active,
                updated_at: explicit.updated_at,
                provenance,
                ..inferred
            }
        );
        contribute(
            &runtime,
            "I prefer spaces.",
            "indentation",
            ContributionScope::User,
        );
        assert_eq!(
            runtime.list(ListMemoryRequest::default()).unwrap().data,
            vec![explicit.clone()]
        );
        assert_recallable(&runtime, &[explicit]);
        let projection = std::fs::read_to_string(root.path().join("user/MEMORY.md")).unwrap();
        assert!(projection.contains(normalized_display));
        assert!(!projection.contains("I prefer spaces"));
    }
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8, candidate retention.
/// Verifies: pruning extraction history cannot remove live conflict authority, including after restart.
#[tokio::test]
async fn proposal_authority_survives_candidate_pruning_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(
        &runtime,
        "I prefer tabs",
        "indentation",
        ContributionScope::User,
    );
    let explicit = remember(&runtime, "i prefer tabs").await;
    runtime
        .connection
        .lock()
        .unwrap()
        .execute("DELETE FROM memory_candidates", [])
        .unwrap();
    drop(runtime);
    let runtime = open_runtime(root.path());
    contribute(
        &runtime,
        "I prefer spaces",
        "indentation",
        ContributionScope::User,
    );
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data,
        vec![explicit.clone()]
    );
    assert_recallable(&runtime, &[explicit]);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8.
/// Verifies: explicit resolution can select either claim, without allowing later inference to undo it.
#[tokio::test]
async fn explicit_conflict_resolution_retains_both_claim_associations() {
    for chosen in ["i prefer tabs", "i prefer spaces"] {
        let root = tempfile::tempdir().unwrap();
        let runtime = open_runtime(root.path());
        contribute(
            &runtime,
            "I prefer tabs.",
            "indentation",
            ContributionScope::User,
        );
        contribute(
            &runtime,
            "I prefer spaces.",
            "indentation",
            ContributionScope::User,
        );
        let explicit = remember(&runtime, chosen).await;
        runtime
            .connection
            .lock()
            .unwrap()
            .execute("DELETE FROM memory_candidates", [])
            .unwrap();
        drop(runtime);
        let runtime = open_runtime(root.path());
        let tabs = contribute(
            &runtime,
            "I prefer tabs.",
            "indentation",
            ContributionScope::User,
        );
        let spaces = contribute(
            &runtime,
            "I prefer spaces.",
            "indentation",
            ContributionScope::User,
        );
        let supporting = match chosen {
            "i prefer tabs" => tabs,
            "i prefer spaces" => spaces,
            _ => unreachable!("test choices"),
        };
        let mut expected = explicit;
        expected.updated_at = chrono::DateTime::from_timestamp_millis(
            (supporting.observed_at + Duration::hours(7)).timestamp_millis(),
        )
        .unwrap();
        expected.provenance.insert(
            expected.provenance.len() - 1,
            MemoryProvenance {
                source_session_id: Some(supporting.session_id.to_string()),
                source_turn_id: Some(supporting.messages[0].turn_id.to_string()),
                source_user_item_id: Some(supporting.messages[0].item_id.clone()),
            },
        );
        let recalled = runtime
            .list_recallable(ListMemoryRequest {
                state: Some(MemoryState::Active),
                ..ListMemoryRequest::default()
            })
            .unwrap()
            .data;
        assert_eq!(recalled, vec![expected]);
        assert_recallable(&runtime, &recalled);
    }
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8, schema version 6.
/// Verifies: a v5 display change is repaired without rerunning the v5 identity migration.
#[tokio::test]
async fn v5_display_update_backfills_proposal_authority_idempotently() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(
        &runtime,
        "I prefer tabs.",
        "indentation",
        ContributionScope::User,
    );
    let explicit = remember(&runtime, "i prefer tabs").await;
    runtime
        .connection
        .lock()
        .unwrap()
        .execute_batch(
            "DROP TABLE IF EXISTS memory_proposal_claims;
         UPDATE memory_schema_meta SET value = '5' WHERE key = 'schema_version';
         CREATE TRIGGER reject_identity_rewrite BEFORE UPDATE OF normalized_key ON memory_entries
         BEGIN SELECT RAISE(ABORT, 'v5 identity must not be migrated again'); END;",
        )
        .unwrap();
    drop(runtime);
    let runtime = open_runtime(root.path());
    contribute(
        &runtime,
        "I prefer spaces.",
        "indentation",
        ContributionScope::User,
    );
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data,
        vec![explicit.clone()]
    );
    assert_recallable(&runtime, std::slice::from_ref(&explicit));
    runtime
        .connection
        .lock()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_second_version_write BEFORE UPDATE ON memory_schema_meta
         BEGIN SELECT RAISE(ABORT, 'current startup must not migrate again'); END;",
        )
        .unwrap();
    drop(runtime);
    let runtime = open_runtime(root.path());
    assert_recallable(&runtime, &[explicit]);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8.
/// Verifies: old active inferred competitors become inspectable conflicts, retaining their source evidence.
#[tokio::test]
async fn v5_active_competitor_is_withheld_without_merging_claims() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(
        &runtime,
        "I prefer tabs.",
        "indentation",
        ContributionScope::User,
    );
    let explicit = remember(&runtime, "i prefer tabs").await;
    install_v5_active_competitor(&runtime);
    let mut opposing = runtime
        .list(ListMemoryRequest::default())
        .unwrap()
        .data
        .into_iter()
        .find(|entry| entry.entry_id.as_str() == "historical-opposing")
        .unwrap();
    opposing.state = MemoryState::Conflicted;
    drop(runtime);
    let runtime = open_runtime(root.path());
    let actual = runtime.list(ListMemoryRequest::default()).unwrap().data;
    assert_eq!(actual, vec![explicit.clone(), opposing]);
    assert_recallable(&runtime, &[explicit]);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8.
/// Verifies: canonical/legacy identity merging redirects old proposal links before deleting the duplicate.
#[tokio::test]
async fn identity_merge_redirects_proposals_from_the_deleted_entry() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(&runtime, "FOO=1", "build-option", ContributionScope::User);
    runtime.connection.lock().unwrap().execute_batch(
        "UPDATE memory_entries SET normalized_key = 'foo1';
         INSERT INTO memory_entries(entry_id, scope_type, scope_id, kind, normalized_key, body, origin, state, created_at, updated_at)
         VALUES('canonical-explicit', 'user', 'user', 'preference', 'FOO=1', 'FOO=1', 'explicit_user', 'active', '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z');
         INSERT INTO memory_entries_fts(entry_id, normalized_key, body) VALUES('canonical-explicit', 'FOO=1', 'FOO=1');"
    ).unwrap();
    contribute(&runtime, "FOO=1", "new-label", ContributionScope::User);
    let explicit = runtime
        .list(ListMemoryRequest::default())
        .unwrap()
        .data
        .remove(0);
    runtime
        .connection
        .lock()
        .unwrap()
        .execute("DELETE FROM memory_candidates", [])
        .unwrap();
    contribute(&runtime, "FOO=2", "build-option", ContributionScope::User);
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data,
        vec![explicit.clone()]
    );
    assert_recallable(&runtime, &[explicit]);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8 scoped identity.
/// Verifies: an identical model label in another scope does not acquire User authority.
#[tokio::test]
async fn proposal_groups_do_not_alias_across_scopes() {
    let root = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(
        &runtime,
        "I prefer tabs",
        "indentation",
        ContributionScope::User,
    );
    let explicit = remember(&runtime, "i prefer tabs").await;
    let source = contribute(
        &runtime,
        "I prefer spaces",
        "indentation",
        ContributionScope::Project(project.path()),
    );
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data,
        vec![explicit]
    );
    let projects = runtime
        .list(ListMemoryRequest {
            scope: Some(MemoryScope::Project),
            workspace_root: project.path().to_path_buf(),
            ..ListMemoryRequest::default()
        })
        .unwrap()
        .data;
    let timestamp = chrono::DateTime::from_timestamp_millis(
        (source.observed_at + Duration::hours(7)).timestamp_millis(),
    )
    .unwrap();
    assert_eq!(
        projects,
        vec![MemoryEntry {
            entry_id: projects[0].entry_id.clone(),
            scope: MemoryScope::Project,
            scope_id: projects[0].scope_id.clone(),
            kind: devo_protocol::native::rpc_memory::MemoryKind::Preference,
            normalized_key: "i prefer spaces".into(),
            body: "I prefer spaces".into(),
            origin: MemoryOrigin::InferredSession,
            state: MemoryState::Active,
            created_at: timestamp,
            updated_at: timestamp,
            replacement_entry_id: None,
            provenance: vec![MemoryProvenance {
                source_session_id: Some(source.session_id.to_string()),
                source_turn_id: Some(source.messages[0].turn_id.to_string()),
                source_user_item_id: Some(source.messages[0].item_id.clone()),
            }],
        }]
    );
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-9.
/// Verifies: a retained proposal association cannot resurrect a revoked identity or attach old evidence.
#[tokio::test]
async fn retained_proposal_does_not_bypass_revocation() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(
        &runtime,
        "I prefer tabs",
        "indentation",
        ContributionScope::User,
    );
    let entry = remember(&runtime, "i prefer tabs").await;
    let prepared = prepare_forget(
        &runtime,
        forget_request(MemoryForgetSelector::EntryId(entry.entry_id)),
    )
    .await
    .unwrap();
    runtime
        .execute_command(MemoryCommand::Forget(prepared))
        .await
        .unwrap();
    let before = runtime.list(ListMemoryRequest::default()).unwrap().data;
    contribute(
        &runtime,
        "I prefer tabs.",
        "indentation",
        ContributionScope::User,
    );
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data,
        before
    );
    assert_recallable(&runtime, &[]);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-4/DD-8.
/// Verifies: failure at the final version write rolls back new relations, withholding, and schema creation.
#[tokio::test]
async fn proposal_migration_failure_rolls_back_and_can_retry() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(
        &runtime,
        "I prefer tabs.",
        "indentation",
        ContributionScope::User,
    );
    let explicit = remember(&runtime, "i prefer tabs").await;
    install_v5_active_competitor(&runtime);
    runtime.rebuild_projections().unwrap();
    let before = runtime.list(ListMemoryRequest::default()).unwrap().data;
    let projection = std::fs::read(root.path().join("user/MEMORY.md")).unwrap();
    runtime
        .connection
        .lock()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_proposal_version BEFORE UPDATE ON memory_schema_meta
         BEGIN SELECT RAISE(ABORT, 'proposal migration rollback'); END;",
        )
        .unwrap();
    drop(runtime);
    let failed = MemoryRuntime::open(
        root.path().to_path_buf(),
        devo_core::MemoryConfig {
            enabled: true,
            ..devo_core::MemoryConfig::default()
        },
    );
    assert!(matches!(failed, Err(super::MemoryError::Database(_))));
    let connection = rusqlite::Connection::open(root.path().join("memory.sqlite3")).unwrap();
    let snapshot = connection
        .query_row(
            "SELECT (SELECT value FROM memory_schema_meta WHERE key = 'schema_version'),
            EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'memory_proposal_claims'),
            (SELECT COUNT(*) FROM memory_entries), (SELECT COUNT(*) FROM memory_evidence),
            (SELECT COUNT(*) FROM memory_entries_fts),
            (SELECT state FROM memory_entries WHERE entry_id = 'historical-opposing')",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, bool>(1)?,
                    row.get::<_, u32>(2)?,
                    row.get::<_, u32>(3)?,
                    row.get::<_, u32>(4)?,
                    row.get::<_, String>(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(snapshot, ("5".into(), false, 2, 3, 2, "active".into()));
    assert_eq!(
        std::fs::read(root.path().join("user/MEMORY.md")).unwrap(),
        projection
    );
    let retained = before
        .iter()
        .map(|entry| {
            super::entries::load_entry(&connection, &entry.entry_id)
                .unwrap()
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(retained, before);
    connection
        .execute_batch("DROP TRIGGER reject_proposal_version")
        .unwrap();
    drop(connection);
    let runtime = open_runtime(root.path());
    contribute(
        &runtime,
        "I prefer spaces.",
        "indentation",
        ContributionScope::User,
    );
    assert_eq!(
        runtime
            .list_recallable(ListMemoryRequest {
                state: Some(MemoryState::Active),
                ..ListMemoryRequest::default()
            })
            .unwrap()
            .data,
        vec![explicit.clone()]
    );
    assert_recallable(&runtime, &[explicit]);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-9.
/// Verifies: reset skips old source claims before they can affect durable proposal state.
#[test]
fn reset_watermark_blocks_old_proposal_replay() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    runtime
        .connection
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO memory_scope_state(scope_type, scope_id, ignore_sources_before)
         VALUES('user', 'user', ?1)",
            [Utc::now().to_rfc3339()],
        )
        .unwrap();
    contribute(
        &runtime,
        "I prefer tabs",
        "indentation",
        ContributionScope::User,
    );
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data,
        vec![]
    );
    assert_recallable(&runtime, &[]);
    let claims: u32 = runtime
        .connection
        .lock()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM memory_proposal_claims", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(claims, 0);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8 conservative historical identity.
/// Verifies: ambiguous history remains unbound until a trusted explicit write resolves its canonical entry.
#[tokio::test]
async fn ambiguous_history_is_not_bound_by_guessing() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(
        &runtime,
        "I prefer tabs",
        "indentation",
        ContributionScope::User,
    );
    let original = runtime
        .list(ListMemoryRequest::default())
        .unwrap()
        .data
        .remove(0);
    runtime.connection.lock().unwrap().execute_batch(
        "INSERT INTO memory_entries(entry_id, scope_type, scope_id, kind, normalized_key, body, origin, state, created_at, updated_at)
         VALUES('ambiguous-legacy', 'user', 'user', 'preference', 'opaque-legacy', 'I prefer tabs', 'inferred_session', 'active', '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z');
         INSERT INTO memory_entries_fts(entry_id, normalized_key, body) VALUES('ambiguous-legacy', 'opaque-legacy', 'I prefer tabs');
         DROP TABLE memory_proposal_claims;
         UPDATE memory_schema_meta SET value = '5' WHERE key = 'schema_version';"
    ).unwrap();
    let before = runtime.list(ListMemoryRequest::default()).unwrap().data;
    drop(runtime);
    let runtime = open_runtime(root.path());
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data,
        before
    );
    let claims = runtime
        .connection
        .lock()
        .unwrap()
        .prepare("SELECT proposal_key, canonical_key, entry_id FROM memory_proposal_claims")
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        claims,
        vec![("indentation".into(), "i prefer tabs".into(), None)]
    );
    let explicit = remember(&runtime, "i prefer tabs").await;
    assert_eq!(explicit.entry_id, original.entry_id);
    contribute(
        &runtime,
        "I prefer spaces",
        "indentation",
        ContributionScope::User,
    );
    let explicit_after =
        super::entries::load_entry(&runtime.connection.lock().unwrap(), &explicit.entry_id)
            .unwrap()
            .unwrap();
    assert_eq!(explicit_after, explicit);
    assert_eq!(
        runtime
            .list(ListMemoryRequest::default())
            .unwrap()
            .data
            .len(),
        2
    );
}

#[path = "proposal_admission_tests.rs"]
mod admission_tests;
