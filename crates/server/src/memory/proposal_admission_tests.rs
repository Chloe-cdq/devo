use super::*;
use pretty_assertions::assert_eq;

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: an excluded claim in one group cannot conflict with clean support in another.
#[test]
fn excluding_cross_group_claim_restores_clean_inferred_entry() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let excluded = contribute(
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
    contribute(
        &runtime,
        "I prefer tabs.",
        "formatting",
        ContributionScope::User,
    );
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data[0].state,
        MemoryState::Conflicted
    );

    runtime
        .exclude_sources(
            &[devo_protocol::SessionId::try_from(excluded.session_id.as_str()).unwrap()],
            Utc::now(),
        )
        .unwrap();
    let entries = runtime.list(ListMemoryRequest::default()).unwrap().data;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].state, MemoryState::Active);
    assert_recallable(&runtime, &entries);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: a new clean group can reactivate an identity after its old group is excluded.
#[test]
fn clean_cross_group_claim_reactivates_retired_inferred_entry() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let excluded = contribute(
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
    runtime
        .exclude_sources(
            &[devo_protocol::SessionId::try_from(excluded.session_id.as_str()).unwrap()],
            Utc::now(),
        )
        .unwrap();

    contribute(
        &runtime,
        "I prefer tabs.",
        "formatting",
        ContributionScope::User,
    );
    let entries = runtime.list(ListMemoryRequest::default()).unwrap().data;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].state, MemoryState::Active);
    assert_recallable(&runtime, &entries);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: deleting an owned claim still checks clean support in another group.
#[test]
fn deleting_cross_group_owned_claim_restores_surviving_inferred_entry() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let deleted = contribute(
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
    contribute(
        &runtime,
        "I prefer tabs.",
        "formatting",
        ContributionScope::User,
    );
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data[0].state,
        MemoryState::Conflicted
    );

    runtime
        .delete_sources(
            &[devo_protocol::SessionId::try_from(deleted.session_id.as_str()).unwrap()],
            Utc::now(),
            devo_protocol::native::rpc_session::RelatedMemoryDeletion::Preserve,
        )
        .unwrap();
    let entries = runtime.list(ListMemoryRequest::default()).unwrap().data;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].state, MemoryState::Active);
    assert_recallable(&runtime, &entries);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8.
/// Verifies: an unbound opposing claim cannot gain recall by changing its model proposal label.
#[tokio::test]
async fn unbound_opposition_cannot_escape_known_authority_with_a_new_label() {
    for explicit_authority in [false, true] {
        for opposing_display in [
            "I prefer spaces.",
            "(i prefer spaces)!",
            "  I  prefer spaces  ",
        ] {
            let root = tempfile::tempdir().unwrap();
            let runtime = open_runtime(root.path());
            contribute(
                &runtime,
                "I prefer tabs.",
                "indentation",
                ContributionScope::User,
            );
            let authority = if explicit_authority {
                remember(&runtime, "I prefer tabs.").await
            } else {
                runtime
                    .list(ListMemoryRequest::default())
                    .unwrap()
                    .data
                    .remove(0)
            };
            contribute(
                &runtime,
                "I prefer spaces.",
                "indentation",
                ContributionScope::User,
            );
            let expected = if explicit_authority {
                vec![authority.clone()]
            } else {
                vec![MemoryEntry {
                    state: MemoryState::Conflicted,
                    updated_at: runtime.list(ListMemoryRequest::default()).unwrap().data[0]
                        .updated_at,
                    ..authority.clone()
                }]
            };
            assert_eq!(
                runtime.list(ListMemoryRequest::default()).unwrap().data,
                expected
            );
            runtime
                .connection
                .lock()
                .unwrap()
                .execute("DELETE FROM memory_candidates", [])
                .unwrap();
            drop(runtime);
            let runtime = open_runtime(root.path());
            for label in ["whitespace", "formatting", "indentation"] {
                contribute(&runtime, opposing_display, label, ContributionScope::User);
                assert_eq!(
                    runtime.list(ListMemoryRequest::default()).unwrap().data,
                    expected
                );
                if explicit_authority {
                    assert_recallable(&runtime, std::slice::from_ref(&authority));
                } else {
                    assert_recallable(&runtime, &[]);
                }
                let projection =
                    std::fs::read_to_string(root.path().join("user/MEMORY.md")).unwrap();
                assert_eq!(projection.contains("prefer spaces"), !explicit_authority);
            }
        }
    }
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8.
/// Verifies: explicitly selecting the previously unbound opposition binds all retained labels.
#[tokio::test]
async fn explicit_selection_of_unbound_opposition_controls_all_its_labels() {
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
    contribute(
        &runtime,
        "I prefer spaces.",
        "whitespace",
        ContributionScope::User,
    );
    assert_recallable(&runtime, &[]);
    let explicit = remember(&runtime, "i prefer spaces").await;
    assert_recallable(&runtime, std::slice::from_ref(&explicit));
    contribute(
        &runtime,
        "I prefer tabs.",
        "formatting",
        ContributionScope::User,
    );
    assert_recallable(&runtime, std::slice::from_ref(&explicit));
}

fn install_v6_key_drift(runtime: &MemoryRuntime) {
    runtime.connection.lock().unwrap().execute_batch(
        "INSERT INTO memory_entries(entry_id, scope_type, scope_id, kind, normalized_key, body, origin, state, created_at, updated_at)
         VALUES('drifted-opposition', 'user', 'user', 'preference', 'i prefer spaces', 'I prefer spaces.', 'inferred_session', 'active', '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z');
         INSERT INTO memory_entries_fts(entry_id, normalized_key, body) VALUES('drifted-opposition', 'i prefer spaces', 'I prefer spaces.');
         INSERT INTO memory_evidence VALUES('drifted-evidence', 'drifted-opposition', 'drifted-source', 'drifted-turn', NULL, '2026-09-01T00:00:00Z', 'drifted-watermark');
         INSERT INTO memory_proposal_claims(scope_type, scope_id, proposal_key, canonical_key, entry_id)
         VALUES('user', 'user', 'whitespace', 'i prefer spaces', 'drifted-opposition');
         DELETE FROM memory_candidates;
         UPDATE memory_schema_meta SET value = '6' WHERE key = 'schema_version';"
    ).unwrap();
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-4, DD-8.
/// Verifies: v6 repair uses retained unbound claims after candidate pruning, without changing identities or evidence.
#[tokio::test]
async fn v6_key_drift_repair_rebinds_all_memberships_and_withholds_unsafe_recall() {
    for explicit_authority in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let runtime = open_runtime(root.path());
        contribute(
            &runtime,
            "I prefer tabs.",
            "indentation",
            ContributionScope::User,
        );
        let authority = if explicit_authority {
            Some(remember(&runtime, "I prefer tabs.").await)
        } else {
            None
        };
        contribute(
            &runtime,
            "I prefer spaces.",
            "indentation",
            ContributionScope::User,
        );
        install_v6_key_drift(&runtime);
        let expected = runtime
            .list(ListMemoryRequest::default())
            .unwrap()
            .data
            .into_iter()
            .map(|entry| {
                if entry.entry_id.as_str() == "drifted-opposition" {
                    MemoryEntry {
                        state: MemoryState::Conflicted,
                        ..entry
                    }
                } else {
                    entry
                }
            })
            .collect::<Vec<_>>();
        drop(runtime);
        let runtime = open_runtime(root.path());
        assert_eq!(
            runtime.list(ListMemoryRequest::default()).unwrap().data,
            expected
        );
        assert_recallable(&runtime, authority.as_slice());
        let snapshot = {
            let connection = runtime.connection.lock().unwrap();
            let bindings = connection.prepare("SELECT proposal_key, canonical_key, entry_id FROM memory_proposal_claims WHERE canonical_key = 'i prefer spaces' ORDER BY proposal_key").unwrap().query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, Option<String>>(2)?))).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
            let version: String = connection
                .query_row(
                    "SELECT value FROM memory_schema_meta WHERE key = 'schema_version'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            (version, bindings)
        };
        assert_eq!(
            snapshot,
            (
                "10".into(),
                vec![
                    (
                        "indentation".into(),
                        "i prefer spaces".into(),
                        Some("drifted-opposition".into())
                    ),
                    (
                        "whitespace".into(),
                        "i prefer spaces".into(),
                        Some("drifted-opposition".into())
                    )
                ]
            )
        );
        let projection = std::fs::read(root.path().join("user/MEMORY.md")).unwrap();
        drop(runtime);
        let runtime = open_runtime(root.path());
        assert_eq!(
            runtime.list(ListMemoryRequest::default()).unwrap().data,
            expected
        );
        assert_eq!(
            std::fs::read(root.path().join("user/MEMORY.md")).unwrap(),
            projection
        );
        contribute(
            &runtime,
            "(I prefer spaces)!",
            "formatting",
            ContributionScope::User,
        );
        assert_recallable(&runtime, authority.as_slice());
    }
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-4, DD-8 transactional repair.
/// Verifies: failed v7 commit rolls back bindings, state, evidence and FTS; retry succeeds.
#[tokio::test]
async fn v7_repair_failure_preserves_all_authoritative_state_until_retry() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(
        &runtime,
        "I prefer tabs.",
        "indentation",
        ContributionScope::User,
    );
    let explicit = remember(&runtime, "I prefer tabs.").await;
    contribute(
        &runtime,
        "I prefer spaces.",
        "indentation",
        ContributionScope::User,
    );
    install_v6_key_drift(&runtime);
    runtime.rebuild_projections().unwrap();
    let before = runtime.list(ListMemoryRequest::default()).unwrap().data;
    let projection = std::fs::read(root.path().join("user/MEMORY.md")).unwrap();
    {
        let connection = runtime.connection.lock().unwrap();
        connection.execute_batch("CREATE TRIGGER reject_v7 BEFORE UPDATE ON memory_schema_meta BEGIN SELECT RAISE(ABORT, 'repair rollback'); END;").unwrap();
        assert!(matches!(
            super::super::schema::create_schema(&connection),
            Err(super::super::MemoryError::Database(_))
        ));
        let snapshot = connection.query_row("SELECT (SELECT value FROM memory_schema_meta WHERE key='schema_version'), (SELECT state FROM memory_entries WHERE entry_id='drifted-opposition'), (SELECT entry_id FROM memory_proposal_claims WHERE proposal_key='indentation' AND canonical_key='i prefer spaces'), (SELECT COUNT(*) FROM memory_entries_fts)", [], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, Option<String>>(2)?, row.get::<_, u32>(3)?))).unwrap();
        assert_eq!(snapshot, ("6".into(), "active".into(), None, 2));
        connection.execute_batch("DROP TRIGGER reject_v7").unwrap();
    }
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data,
        before
    );
    assert_eq!(
        std::fs::read(root.path().join("user/MEMORY.md")).unwrap(),
        projection
    );
    drop(runtime);
    let runtime = open_runtime(root.path());
    let expected = before
        .into_iter()
        .map(|entry| {
            if entry.entry_id.as_str() == "drifted-opposition" {
                MemoryEntry {
                    state: MemoryState::Conflicted,
                    ..entry
                }
            } else {
                entry
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data,
        expected
    );
    assert_recallable(&runtime, &[explicit]);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8 scoped identity.
/// Verifies: retained unbound conflicts constrain only their own scope, across label changes.
#[tokio::test]
async fn key_drift_admission_does_not_share_authority_between_scopes() {
    let root = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(
        &runtime,
        "I prefer tabs.",
        "indentation",
        ContributionScope::User,
    );
    let explicit = remember(&runtime, "I prefer tabs.").await;
    contribute(
        &runtime,
        "I prefer spaces.",
        "indentation",
        ContributionScope::User,
    );
    contribute(
        &runtime,
        "I prefer spaces.",
        "whitespace",
        ContributionScope::User,
    );
    assert_recallable(&runtime, std::slice::from_ref(&explicit));
    let source = contribute(
        &runtime,
        "I prefer spaces.",
        "whitespace",
        ContributionScope::Project(project.path()),
    );
    let entries = runtime
        .list(ListMemoryRequest {
            scope: Some(MemoryScope::Project),
            workspace_root: source.workspace_root,
            ..ListMemoryRequest::default()
        })
        .unwrap()
        .data;
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0],
        MemoryEntry {
            scope: MemoryScope::Project,
            body: "I prefer spaces.".into(),
            normalized_key: "i prefer spaces".into(),
            origin: MemoryOrigin::InferredSession,
            state: MemoryState::Active,
            ..entries[0].clone()
        }
    );
    let connection = runtime.connection.lock().unwrap();
    let indexed: Vec<String> = connection
        .prepare("SELECT body FROM memory_entries_fts ORDER BY body")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(indexed, vec!["I prefer spaces.", "I prefer tabs."]);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8, DD-9.
/// Verifies: forgetting the authority never revives an already-conflicted inferred entry; explicit selection can resolve it.
#[tokio::test]
async fn forgetting_authority_does_not_let_inference_resolve_retained_conflict() {
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
    contribute(
        &runtime,
        "I prefer spaces.",
        "whitespace",
        ContributionScope::User,
    );
    assert_recallable(&runtime, &[]);
    let explicit = remember(&runtime, "I prefer spaces.").await;
    let prepared = prepare_forget(
        &runtime,
        forget_request(MemoryForgetSelector::EntryId(explicit.entry_id.clone())),
    )
    .await
    .unwrap();
    runtime
        .execute_command(MemoryCommand::Forget(prepared))
        .await
        .unwrap();
    assert_recallable(&runtime, &[]);
    contribute(
        &runtime,
        "I prefer tabs.",
        "formatting",
        ContributionScope::User,
    );
    assert_recallable(&runtime, &[]);
    let restored = remember(&runtime, "I prefer spaces.").await;
    assert_eq!(
        restored,
        MemoryEntry {
            state: MemoryState::Restored,
            updated_at: restored.updated_at,
            provenance: restored.provenance.clone(),
            ..explicit
        }
    );
    assert_recallable(&runtime, &[restored]);
}
