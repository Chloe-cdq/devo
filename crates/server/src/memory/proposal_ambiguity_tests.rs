use super::*;
use pretty_assertions::assert_eq;
use rusqlite::types::Value;

#[derive(Clone, Copy)]
enum HistoricalSchema {
    V5,
    V6,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Opponent {
    None,
    UnboundInferred,
    Explicit,
}

fn seed_ambiguity(
    runtime: &MemoryRuntime,
    project: &Path,
    schema: HistoricalSchema,
    opponent: Opponent,
    duplicate_state: MemoryState,
) -> Vec<MemoryEntry> {
    contribute(
        runtime,
        "I prefer tabs.",
        "indentation",
        ContributionScope::User,
    );
    contribute(runtime, "use rust", "language", ContributionScope::User);
    contribute(
        runtime,
        "I prefer tabs.",
        "indentation",
        ContributionScope::Project(project),
    );
    let connection = runtime.connection.lock().unwrap();
    connection.execute(
        "INSERT INTO memory_entries(entry_id,scope_type,scope_id,kind,normalized_key,body,origin,state,created_at,updated_at)
         VALUES('ambiguous-duplicate','user','user','preference','opaque-legacy','(I prefer tabs)!','inferred_session',?1,'2026-09-01T00:00:00Z','2026-09-01T00:00:00Z')",
        [crate::memory::state_name(duplicate_state)],
    ).unwrap();
    connection.execute_batch(
        "INSERT INTO memory_entries_fts(entry_id,normalized_key,body)
         VALUES('ambiguous-duplicate','opaque-legacy','(I prefer tabs)!');
         INSERT INTO memory_evidence(evidence_id,entry_id,session_id,turn_id,source_user_item_id,observed_at,source_watermark)
         VALUES('ambiguous-evidence','ambiguous-duplicate','legacy-source','legacy-turn',NULL,'2026-09-01T00:00:00Z','legacy-watermark');"
    ).unwrap();
    if opponent == Opponent::Explicit {
        connection.execute_batch(
            "INSERT INTO memory_entries(entry_id,scope_type,scope_id,kind,normalized_key,body,origin,state,created_at,updated_at)
             VALUES('explicit-opponent','user','user','preference','i prefer spaces','I prefer spaces.','explicit_user','active','2026-09-01T00:00:00Z','2026-09-01T00:00:00Z');
             INSERT INTO memory_entries_fts(entry_id,normalized_key,body)
             VALUES('explicit-opponent','i prefer spaces','I prefer spaces.');
             INSERT INTO memory_evidence(evidence_id,entry_id,session_id,observed_at,source_watermark)
             VALUES('explicit-evidence','explicit-opponent','explicit-source','2026-09-01T00:00:00Z','explicit-watermark');"
        ).unwrap();
    }
    if opponent != Opponent::None {
        let binding = match opponent {
            Opponent::Explicit => Some("explicit-opponent"),
            Opponent::UnboundInferred => None,
            Opponent::None => unreachable!(),
        };
        connection.execute(
            "INSERT INTO memory_proposal_claims(scope_type,scope_id,proposal_key,canonical_key,entry_id)
             VALUES('user','user','indentation','i prefer spaces',?1)",
            [binding],
        ).unwrap();
        connection.execute_batch(
            "INSERT INTO memory_candidates(candidate_id,scope_type,scope_id,kind,normalized_key,body,origin,source_session_id,validation_outcome,retention_until,created_at)
             VALUES('opposing-candidate','user','user','preference','indentation','I prefer spaces.','inferred_session','opposing-source','conflicted','2027-01-01T00:00:00Z','2026-09-01T00:00:00Z');"
        ).unwrap();
    }
    match schema {
        HistoricalSchema::V5 => connection
            .execute_batch(
                "DROP TABLE memory_proposal_claims;
             UPDATE memory_schema_meta SET value='5' WHERE key='schema_version';",
            )
            .unwrap(),
        HistoricalSchema::V6 => connection
            .execute_batch(
                "DELETE FROM memory_candidates;
             UPDATE memory_schema_meta SET value='6' WHERE key='schema_version';",
            )
            .unwrap(),
    }
    drop(connection);
    let mut entries = runtime.list(ListMemoryRequest::default()).unwrap().data;
    entries.sort_by(|a, b| a.entry_id.cmp(&b.entry_id));
    entries
}

fn evidence_snapshot(runtime: &MemoryRuntime) -> Vec<Vec<Value>> {
    let connection = runtime.connection.lock().unwrap();
    let mut statement = connection
        .prepare("SELECT * FROM memory_evidence ORDER BY evidence_id")
        .unwrap();
    let columns = statement.column_count();
    statement
        .query_map([], |row| {
            (0..columns)
                .map(|index| row.get(index))
                .collect::<Result<Vec<Value>, _>>()
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

enum TabsBinding<'a> {
    Unbound,
    Bound(&'a str),
}

fn assert_ambiguity_state(
    runtime: &MemoryRuntime,
    expected: &[MemoryEntry],
    project: &Path,
    project_before: &[MemoryEntry],
    evidence_before: &[Vec<Value>],
    tabs_binding: TabsBinding<'_>,
) {
    let mut actual = runtime.list(ListMemoryRequest::default()).unwrap().data;
    actual.sort_by(|a, b| a.entry_id.cmp(&b.entry_id));
    assert_eq!(actual, expected);
    let mut recalled = runtime
        .list_recallable(ListMemoryRequest {
            state: Some(MemoryState::Active),
            ..ListMemoryRequest::default()
        })
        .unwrap()
        .data;
    recalled.sort_by(|a, b| a.entry_id.cmp(&b.entry_id));
    let expected_recall = expected
        .iter()
        .filter(|entry| matches!(entry.state, MemoryState::Active | MemoryState::Restored))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(recalled, expected_recall);
    let projects = runtime
        .list_recallable(ListMemoryRequest {
            scope: Some(MemoryScope::Project),
            state: Some(MemoryState::Active),
            workspace_root: project.to_path_buf(),
            ..ListMemoryRequest::default()
        })
        .unwrap()
        .data;
    assert_eq!(projects, project_before);
    assert_eq!(evidence_snapshot(runtime), evidence_before);
    let connection = runtime.connection.lock().unwrap();
    let bindings = connection.prepare(
        "SELECT proposal_key,canonical_key,entry_id FROM memory_proposal_claims
         WHERE scope_type='user' AND scope_id='user' AND canonical_key='i prefer tabs' ORDER BY proposal_key"
    ).unwrap().query_map([],|row| Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?,row.get::<_,Option<String>>(2)?))).unwrap().collect::<Result<Vec<_>,_>>().unwrap();
    let binding = match tabs_binding {
        TabsBinding::Unbound => None,
        TabsBinding::Bound(entry_id) => Some(entry_id.to_owned()),
    };
    assert_eq!(
        bindings,
        vec![("indentation".into(), "i prefer tabs".into(), binding)]
    );
    let indexed = connection
        .prepare("SELECT entry_id,normalized_key,body FROM memory_entries_fts ORDER BY entry_id")
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut expected_index = expected_recall
        .iter()
        .chain(project_before)
        .map(|entry| {
            (
                entry.entry_id.to_string(),
                entry.normalized_key.clone(),
                entry.body.clone(),
            )
        })
        .collect::<Vec<_>>();
    expected_index.sort();
    assert_eq!(indexed, expected_index);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-4/DD-8.
/// Verifies: v5/v6 ambiguous canonical matches are withheld without guessing a binding when live opposing claims exist.
#[tokio::test]
async fn ambiguous_v5_v6_claims_with_live_opposition_are_withheld() {
    for schema in [HistoricalSchema::V5, HistoricalSchema::V6] {
        for opponent in [Opponent::UnboundInferred, Opponent::Explicit] {
            for duplicate_state in [MemoryState::Active, MemoryState::Restored] {
                let root = tempfile::tempdir().unwrap();
                let project = tempfile::tempdir().unwrap();
                let runtime = open_runtime(root.path());
                let before =
                    seed_ambiguity(&runtime, project.path(), schema, opponent, duplicate_state);
                let project_before = runtime
                    .list(ListMemoryRequest {
                        scope: Some(MemoryScope::Project),
                        workspace_root: project.path().to_path_buf(),
                        ..ListMemoryRequest::default()
                    })
                    .unwrap()
                    .data;
                let evidence_before = evidence_snapshot(&runtime);
                let expected = before
                    .into_iter()
                    .map(|entry| {
                        if entry.body == "I prefer tabs."
                            || entry.entry_id.as_str() == "ambiguous-duplicate"
                        {
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
                assert_ambiguity_state(
                    &runtime,
                    &expected,
                    project.path(),
                    &project_before,
                    &evidence_before,
                    TabsBinding::Unbound,
                );
                let projection = std::fs::read(root.path().join("user/MEMORY.md")).unwrap();
                drop(runtime);
                let runtime = open_runtime(root.path());
                assert_ambiguity_state(
                    &runtime,
                    &expected,
                    project.path(),
                    &project_before,
                    &evidence_before,
                    TabsBinding::Unbound,
                );
                assert_eq!(
                    std::fs::read(root.path().join("user/MEMORY.md")).unwrap(),
                    projection
                );
            }
        }
    }
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-4/DD-8 conservative identity.
/// Verifies: ambiguity alone preserves both historical inferred entries, keys, evidence, timestamps and recall.
#[tokio::test]
async fn ambiguous_v5_v6_claims_without_opposition_remain_recallable() {
    for schema in [HistoricalSchema::V5, HistoricalSchema::V6] {
        for duplicate_state in [MemoryState::Active, MemoryState::Restored] {
            let root = tempfile::tempdir().unwrap();
            let project = tempfile::tempdir().unwrap();
            let runtime = open_runtime(root.path());
            let before = seed_ambiguity(
                &runtime,
                project.path(),
                schema,
                Opponent::None,
                duplicate_state,
            );
            let project_before = runtime
                .list(ListMemoryRequest {
                    scope: Some(MemoryScope::Project),
                    workspace_root: project.path().to_path_buf(),
                    ..ListMemoryRequest::default()
                })
                .unwrap()
                .data;
            let evidence_before = evidence_snapshot(&runtime);
            drop(runtime);
            let runtime = open_runtime(root.path());
            assert_ambiguity_state(
                &runtime,
                &before,
                project.path(),
                &project_before,
                &evidence_before,
                TabsBinding::Unbound,
            );
            drop(runtime);
            let runtime = open_runtime(root.path());
            assert_ambiguity_state(
                &runtime,
                &before,
                project.path(),
                &project_before,
                &evidence_before,
                TabsBinding::Unbound,
            );
        }
    }
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8.
/// Verifies: rebinding a claim after ambiguous migration cannot leave a duplicate inferred body recalled when opposition later arrives.
#[tokio::test]
async fn ambiguous_rebound_claim_with_new_opposition_withholds_every_matching_entry() {
    let root = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let before = seed_ambiguity(
        &runtime,
        project.path(),
        HistoricalSchema::V6,
        Opponent::None,
        MemoryState::Restored,
    );
    let project_before = runtime
        .list(ListMemoryRequest {
            scope: Some(MemoryScope::Project),
            workspace_root: project.path().to_path_buf(),
            ..ListMemoryRequest::default()
        })
        .unwrap()
        .data;
    let evidence_before = evidence_snapshot(&runtime);
    let canonical_entry_id = before
        .iter()
        .find(|entry| entry.body == "I prefer tabs.")
        .unwrap()
        .entry_id
        .clone();
    drop(runtime);
    let runtime = open_runtime(root.path());
    assert_ambiguity_state(
        &runtime,
        &before,
        project.path(),
        &project_before,
        &evidence_before,
        TabsBinding::Unbound,
    );
    contribute(
        &runtime,
        "I prefer tabs.",
        "indentation",
        ContributionScope::User,
    );
    let mut rebound = runtime.list(ListMemoryRequest::default()).unwrap().data;
    rebound.sort_by(|a, b| a.entry_id.cmp(&b.entry_id));
    let evidence_after_rebind = evidence_snapshot(&runtime);
    assert_ambiguity_state(
        &runtime,
        &rebound,
        project.path(),
        &project_before,
        &evidence_after_rebind,
        TabsBinding::Bound(canonical_entry_id.as_str()),
    );
    let expected = rebound
        .into_iter()
        .map(|entry| {
            if entry.entry_id == canonical_entry_id
                || entry.entry_id.as_str() == "ambiguous-duplicate"
            {
                MemoryEntry {
                    state: MemoryState::Conflicted,
                    ..entry
                }
            } else {
                entry
            }
        })
        .collect::<Vec<_>>();
    for body in ["I prefer spaces.", "i prefer spaces"] {
        contribute(&runtime, body, "indentation", ContributionScope::User);
        assert_ambiguity_state(
            &runtime,
            &expected,
            project.path(),
            &project_before,
            &evidence_after_rebind,
            TabsBinding::Bound(canonical_entry_id.as_str()),
        );
    }
    drop(runtime);
    let runtime = open_runtime(root.path());
    assert_ambiguity_state(
        &runtime,
        &expected,
        project.path(),
        &project_before,
        &evidence_after_rebind,
        TabsBinding::Bound(canonical_entry_id.as_str()),
    );
}
