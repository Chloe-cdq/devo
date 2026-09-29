use super::*;
use pretty_assertions::assert_eq;

fn install_lossy_identity(runtime: &MemoryRuntime) {
    runtime.connection.lock().unwrap().execute_batch(
        "INSERT INTO memory_entries(entry_id, scope_type, scope_id, kind, normalized_key, body, origin, state, created_at, updated_at)
         VALUES('legacy-assignment', 'user', 'user', 'preference', 'foo1', 'FOO=1', 'inferred_session', 'active', '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z');
         INSERT INTO memory_entries_fts(entry_id, normalized_key, body) VALUES('legacy-assignment', 'foo1', 'FOO=1');
         INSERT INTO memory_evidence VALUES('legacy-evidence', 'legacy-assignment', 'legacy-source', 'legacy-turn', NULL, '2026-09-01T00:00:00Z', 'legacy-watermark');
         UPDATE memory_schema_meta SET value = '6' WHERE key = 'schema_version';"
    ).unwrap();
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8.
/// Verifies: a lossy historical storage key cannot attach a different candidate's evidence or claim to its entry.
#[tokio::test]
async fn incompatible_legacy_storage_key_preserves_entry_and_withholds_new_candidate() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    install_lossy_identity(&runtime);
    drop(runtime);
    let runtime = open_runtime(root.path());
    let before = runtime.list(ListMemoryRequest::default()).unwrap().data;
    let projection = std::fs::read(root.path().join("user/MEMORY.md")).unwrap();
    let source = contribute(&runtime, "foo1", "plain-text", ContributionScope::User);
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data,
        before
    );
    assert_recallable(&runtime, &before);
    let snapshot = {
        let connection = runtime.connection.lock().unwrap();
        let candidate = connection
            .query_row(
                "SELECT body, validation_outcome, source_session_id FROM memory_candidates",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .unwrap();
        let claim = connection
            .query_row(
                "SELECT proposal_key, canonical_key, entry_id FROM memory_proposal_claims",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .unwrap();
        (candidate, claim)
    };
    assert_eq!(
        snapshot,
        (
            (
                "foo1".into(),
                "identity_collision".into(),
                source.session_id.to_string()
            ),
            ("plain-text".into(), "foo1".into(), None)
        )
    );
    assert_eq!(
        std::fs::read(root.path().join("user/MEMORY.md")).unwrap(),
        projection
    );
    drop(runtime);
    let runtime = open_runtime(root.path());
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data,
        before
    );
    assert_recallable(&runtime, &before);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8.
/// Verifies: explicit writes refuse a different claim occupying their canonical storage key without overwriting history.
#[tokio::test]
async fn explicit_write_cannot_overwrite_an_incompatible_legacy_storage_identity() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    install_lossy_identity(&runtime);
    drop(runtime);
    let runtime = open_runtime(root.path());
    let before = runtime.list(ListMemoryRequest::default()).unwrap().data;
    let projection = std::fs::read(root.path().join("user/MEMORY.md")).unwrap();
    let result = runtime
        .execute_command(MemoryCommand::Remember(remember_request("foo1")))
        .await;
    assert!(
        matches!(result, Err(super::super::MemoryError::InvalidRequest(_))),
        "{result:?}"
    );
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data,
        before
    );
    assert_recallable(&runtime, &before);
    assert_eq!(
        std::fs::read(root.path().join("user/MEMORY.md")).unwrap(),
        projection
    );
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8.
/// Verifies: authorized equivalent explicit canonicalization frees the colliding key and preserves separate evidence for the new claim.
#[tokio::test]
async fn explicit_canonicalization_releases_legacy_collision_without_merging_claims() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    install_lossy_identity(&runtime);
    drop(runtime);
    let runtime = open_runtime(root.path());
    let before = runtime
        .list(ListMemoryRequest::default())
        .unwrap()
        .data
        .remove(0);
    contribute(&runtime, "foo1", "plain-text", ContributionScope::User);
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data,
        vec![before.clone()]
    );
    let explicit = remember(&runtime, "FOO=1").await;
    assert_eq!(
        explicit,
        MemoryEntry {
            normalized_key: "FOO=1".into(),
            origin: MemoryOrigin::ExplicitUser,
            updated_at: explicit.updated_at,
            provenance: explicit.provenance.clone(),
            ..before
        }
    );
    let source = contribute(
        &runtime,
        "foo1",
        "renamed-plain-text",
        ContributionScope::User,
    );
    let mut entries = runtime.list(ListMemoryRequest::default()).unwrap().data;
    let plain = entries.remove(
        entries
            .iter()
            .position(|entry| entry.entry_id != explicit.entry_id)
            .unwrap(),
    );
    assert_eq!(entries, vec![explicit.clone()]);
    assert_eq!(
        plain,
        MemoryEntry {
            body: "foo1".into(),
            normalized_key: "foo1".into(),
            origin: MemoryOrigin::InferredSession,
            state: MemoryState::Active,
            provenance: vec![MemoryProvenance {
                source_session_id: Some(source.session_id.to_string()),
                source_turn_id: Some(source.messages[0].turn_id.to_string()),
                source_user_item_id: Some(source.messages[0].item_id.clone())
            }],
            ..plain.clone()
        }
    );
    assert_recallable(&runtime, &[explicit, plain.clone()]);
    let bindings = runtime
        .connection
        .lock()
        .unwrap()
        .prepare("SELECT proposal_key, entry_id FROM memory_proposal_claims ORDER BY proposal_key")
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        bindings,
        vec![
            ("plain-text".into(), Some(plain.entry_id.to_string())),
            (
                "renamed-plain-text".into(),
                Some(plain.entry_id.to_string())
            )
        ]
    );
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8.
/// Verifies: an incompatible canonical match prevents merging or redirecting a separately proven legacy match.
#[tokio::test]
async fn mixed_legacy_and_incompatible_canonical_matches_never_merge() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    install_lossy_identity(&runtime);
    runtime.connection.lock().unwrap().execute_batch(
        "INSERT INTO memory_entries(entry_id, scope_type, scope_id, kind, normalized_key, body, origin, state, created_at, updated_at)
         VALUES('incompatible-canonical', 'user', 'user', 'preference', 'FOO=1', 'FOO=2', 'inferred_session', 'active', '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z');
         INSERT INTO memory_entries_fts VALUES('incompatible-canonical', 'FOO=1', 'FOO=2');"
    ).unwrap();
    drop(runtime);
    let runtime = open_runtime(root.path());
    let before = runtime.list(ListMemoryRequest::default()).unwrap().data;
    contribute(&runtime, "FOO=1", "assignment", ContributionScope::User);
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data,
        before
    );
    assert_recallable(&runtime, &before);
    let result = runtime
        .execute_command(MemoryCommand::Remember(remember_request("FOO=1")))
        .await;
    assert!(
        matches!(result, Err(super::super::MemoryError::InvalidRequest(_))),
        "{result:?}"
    );
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data,
        before
    );
    assert_recallable(&runtime, &before);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8 scoped identity.
/// Verifies: a historical User storage collision cannot block an independent Project entry.
#[tokio::test]
async fn incompatible_legacy_storage_collision_stays_within_its_scope() {
    let root = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    install_lossy_identity(&runtime);
    let before = runtime.list(ListMemoryRequest::default()).unwrap().data;
    let source = contribute(
        &runtime,
        "foo1",
        "plain-text",
        ContributionScope::Project(project.path()),
    );
    assert_eq!(
        runtime.list(ListMemoryRequest::default()).unwrap().data,
        before
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
            body: "foo1".into(),
            normalized_key: "foo1".into(),
            origin: MemoryOrigin::InferredSession,
            state: MemoryState::Active,
            ..entries[0].clone()
        }
    );
}
