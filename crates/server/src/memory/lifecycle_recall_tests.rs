use super::*;
use devo_protocol::native::rpc_session::RelatedMemoryDeletion;
use pretty_assertions::assert_eq;
use std::sync::Arc;

/// Trace: L2-DES-MEM-001 Rev 4 Entry Lifecycle and Retention / DD-10.
/// Verifies: foreground reads expire overdue inference before it can renew its own recall time.
#[tokio::test]
async fn overdue_inference_is_withheld_without_a_background_scan() {
    enum Access {
        Recall,
        Search,
        List,
        Read,
    }
    for access in [Access::Recall, Access::Search, Access::List, Access::Read] {
        let root = tempfile::tempdir().unwrap();
        let mut runtime = open_runtime(root.path());
        contribute(&runtime, "I prefer tabs.", "indentation", epoch());
        let original = entries(&runtime).remove(0);
        runtime.clock = Arc::new(|| epoch() + Duration::days(90));
        match access {
            Access::Recall => assert_eq!(recall(&runtime, root.path()).await, vec![]),
            Access::Search => assert_eq!(search(&runtime, /*state*/ None), vec![]),
            Access::List => assert_eq!(
                entries(&runtime),
                vec![MemoryEntry {
                    state: MemoryState::Stale,
                    ..original.clone()
                }]
            ),
            Access::Read => {
                let actual = runtime
                    .read(super::super::ReadMemoryRequest {
                        entry_id: original.entry_id.clone(),
                        workspace_root: root.path().to_path_buf(),
                    })
                    .unwrap();
                assert_eq!(
                    actual,
                    devo_protocol::native::rpc_memory::MemoryReadEntry {
                        entry_id: original.entry_id.clone(),
                        scope: MemoryScope::User,
                        kind: MemoryKind::Preference,
                        state: MemoryState::Stale,
                        body: "I prefer tabs.".into(),
                        source_summary: "Inferred session memory (1 source)".into(),
                    }
                );
            }
        }
        assert_eq!(
            runtime.entry_by_id(&original.entry_id).unwrap(),
            Some(MemoryEntry {
                state: MemoryState::Stale,
                ..original
            })
        );
    }
}

/// Trace: L2-DES-MEM-001 Rev 4 Entry Lifecycle and Retention / DD-10.
/// Verifies: real recall renews a still-valid inferred entry for exactly another 90 days.
#[tokio::test]
async fn foreground_recall_uses_the_injected_clock_for_renewal() {
    let root = tempfile::tempdir().unwrap();
    let mut runtime = open_runtime(root.path());
    contribute(&runtime, "I prefer tabs.", "indentation", epoch());
    let original = entries(&runtime).remove(0);
    runtime.clock = Arc::new(|| epoch() + Duration::days(89));
    let expected = vec![devo_protocol::native::rpc_memory::MemoryRecallEntry {
        entry_id: original.entry_id.clone(),
        scope: MemoryScope::User,
        kind: MemoryKind::Preference,
        summary: "I prefer tabs.".into(),
        source_summary: "Inferred session memory (1 source)".into(),
    }];
    assert_eq!(recall(&runtime, root.path()).await, expected);
    runtime.prune_expired(epoch() + Duration::days(90)).unwrap();
    assert_eq!(entries(&runtime), vec![original.clone()]);
    runtime.clock = Arc::new(|| epoch() + Duration::days(179));
    assert_eq!(recall(&runtime, root.path()).await, vec![]);
    assert_eq!(
        entries(&runtime),
        vec![MemoryEntry {
            state: MemoryState::Stale,
            ..original
        }]
    );
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8 / Entry Lifecycle and Retention.
/// Verifies: validating a stale claim rechecks established competition before making it recallable.
#[test]
fn stale_verification_reconciles_competing_live_inference() {
    let root = tempfile::tempdir().unwrap();
    let mut runtime = open_runtime(root.path());
    contribute(&runtime, "I prefer tabs.", "indentation", epoch());
    let tabs = entries(&runtime).remove(0);
    runtime.clock = Arc::new(|| epoch() + Duration::days(90));
    runtime.prune_expired(epoch() + Duration::days(90)).unwrap();
    contribute(
        &runtime,
        "I prefer spaces.",
        "indentation",
        epoch() + Duration::days(90),
    );
    let spaces = entries(&runtime)
        .into_iter()
        .find(|entry| entry.body == "I prefer spaces.")
        .unwrap();
    let source = contribute(
        &runtime,
        "I prefer tabs.",
        "another model label",
        epoch() + Duration::days(91),
    );
    assert_eq!(
        runtime.entry_by_id(&spaces.entry_id).unwrap(),
        Some(MemoryEntry {
            state: MemoryState::Conflicted,
            ..spaces
        })
    );
    let mut provenance = tabs.provenance.clone();
    provenance.push(devo_protocol::native::rpc_memory::MemoryProvenance {
        source_session_id: Some(source.session_id.to_string()),
        source_turn_id: Some(source.messages[0].turn_id.to_string()),
        source_user_item_id: Some(source.messages[0].item_id.clone()),
    });
    assert_eq!(
        runtime.entry_by_id(&tabs.entry_id).unwrap(),
        Some(MemoryEntry {
            state: MemoryState::Conflicted,
            updated_at: epoch() + Duration::days(91),
            provenance,
            ..tabs
        })
    );
    assert_eq!(search(&runtime, /*state*/ None), vec![]);
}

/// Trace: L2-DES-MEM-001 Rev 4 Entry Lifecycle and Retention.
/// Verifies: reopening expires inferred entries using the supplied clock and leaves explicit knowledge durable.
#[tokio::test]
async fn startup_maintenance_expires_only_inferred_entries() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    contribute(&runtime, "I prefer tabs.", "indentation", epoch());
    let inferred = entries(&runtime).remove(0);
    let explicit = remember(&runtime, "Use Rust.").await;
    drop(runtime);
    let runtime = MemoryRuntime::open_with_clock(
        root.path().to_path_buf(),
        devo_core::MemoryConfig {
            enabled: true,
            ..Default::default()
        },
        Arc::new(|| epoch() + Duration::days(500)),
    )
    .unwrap();
    assert_eq!(
        runtime.entry_by_id(&inferred.entry_id).unwrap(),
        Some(MemoryEntry {
            state: MemoryState::Stale,
            ..inferred
        })
    );
    assert_eq!(
        runtime.entry_by_id(&explicit.entry_id).unwrap(),
        Some(explicit)
    );
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7 / DD-8 / Entry Lifecycle and Retention.
/// Verifies: explicit group authority remains durable when all inference sources are excluded or deleted.
#[tokio::test]
async fn explicit_authority_survives_inferred_source_removal() {
    for change in [SourceChange::Exclude, SourceChange::Delete] {
        let root = tempfile::tempdir().unwrap();
        let runtime = open_runtime(root.path());
        let tabs = contribute(&runtime, "I prefer tabs.", "indentation", epoch());
        let spaces = contribute(
            &runtime,
            "I prefer spaces.",
            "indentation",
            epoch() + Duration::days(1),
        );
        let selected = remember(&runtime, "I prefer spaces.").await;
        let sources = [tabs.session_id, spaces.session_id]
            .into_iter()
            .map(|id| devo_protocol::SessionId::try_from(id.as_str()).unwrap())
            .collect::<Vec<_>>();
        match change {
            SourceChange::Exclude => runtime.exclude_sources(&sources, epoch() + Duration::days(2)),
            SourceChange::Delete => runtime
                .delete_sources(
                    &sources,
                    epoch() + Duration::days(2),
                    RelatedMemoryDeletion::Preserve,
                )
                .map(|_| ()),
        }
        .unwrap();
        contribute(
            &runtime,
            "I prefer two spaces.",
            "indentation",
            epoch() + Duration::days(3),
        );
        assert_eq!(
            search(&runtime, /*state*/ None),
            vec![devo_protocol::native::rpc_memory::MemorySearchEntry {
                entry_id: selected.entry_id.clone(),
                scope: MemoryScope::User,
                kind: MemoryKind::Preference,
                state: MemoryState::Active,
                summary: "I prefer spaces.".into(),
            }]
        );
        let previous = runtime.entry_by_id(&selected.entry_id).unwrap().unwrap();
        let winner = remember(&runtime, "I prefer tabs.").await;
        assert_eq!(
            runtime.entry_by_id(&previous.entry_id).unwrap(),
            Some(MemoryEntry {
                state: MemoryState::Retired,
                replacement_entry_id: Some(winner.entry_id.clone()),
                ..previous
            })
        );
        assert_eq!(
            search(&runtime, /*state*/ None),
            vec![devo_protocol::native::rpc_memory::MemorySearchEntry {
                entry_id: winner.entry_id,
                scope: MemoryScope::User,
                kind: MemoryKind::Preference,
                state: MemoryState::Active,
                summary: "I prefer tabs.".into(),
            }]
        );
    }
}

enum SourceChange {
    Exclude,
    Delete,
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8 / Entry Lifecycle and Retention / DD-10.
/// Verifies: removing opposition cannot renew an overdue surviving inferred claim.
#[tokio::test]
async fn source_reconciliation_does_not_reactivate_overdue_inference() {
    for change in [SourceChange::Exclude, SourceChange::Delete] {
        let root = tempfile::tempdir().unwrap();
        let mut runtime = open_runtime(root.path());
        contribute(&runtime, "I prefer tabs.", "indentation", epoch());
        let original = entries(&runtime).remove(0);
        let opposition = contribute(
            &runtime,
            "I prefer spaces.",
            "indentation",
            epoch() + Duration::days(1),
        );
        runtime.clock = Arc::new(|| epoch() + Duration::days(90));
        let sources = [devo_protocol::SessionId::try_from(opposition.session_id.as_str()).unwrap()];
        match change {
            SourceChange::Exclude => {
                runtime.exclude_sources(&sources, epoch() + Duration::days(90))
            }
            SourceChange::Delete => runtime
                .delete_sources(
                    &sources,
                    epoch() + Duration::days(90),
                    RelatedMemoryDeletion::Preserve,
                )
                .map(|_| ()),
        }
        .unwrap();
        assert_eq!(
            runtime.entry_by_id(&original.entry_id).unwrap(),
            Some(MemoryEntry {
                state: MemoryState::Stale,
                ..original
            })
        );
        assert_eq!(recall(&runtime, root.path()).await, vec![]);
    }
}

/// Trace: L2-DES-MEM-001 Rev 4 Entry Lifecycle and Retention / DD-10.
/// Verifies: recall checks age inside selection even if overdue inference becomes active after expiry snapshots entries.
#[tokio::test]
async fn recall_selection_rechecks_age_after_expiry_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let mut runtime = open_runtime(root.path());
    contribute(&runtime, "I prefer tabs.", "indentation", epoch());
    let target = entries(&runtime).remove(0);
    contribute(&runtime, "I prefer spaces.", "another group", epoch());
    let sentinel = entries(&runtime)
        .into_iter()
        .find(|entry| entry.entry_id != target.entry_id)
        .unwrap();
    let target_id = &target.entry_id;
    let sentinel_id = &sentinel.entry_id;
    runtime.connection.lock().unwrap().execute_batch(&format!(
        "UPDATE memory_entries SET state = 'conflicted' WHERE entry_id = '{target_id}';
         DELETE FROM memory_entries_fts WHERE entry_id = '{target_id}';
         CREATE TRIGGER reactivate_after_expiry_snapshot AFTER UPDATE OF state ON memory_entries
         WHEN NEW.entry_id = '{sentinel_id}' AND NEW.state = 'stale'
         BEGIN
           UPDATE memory_entries SET state = 'active' WHERE entry_id = '{target_id}';
           INSERT INTO memory_entries_fts(entry_id, normalized_key, body)
             SELECT entry_id, normalized_key, body FROM memory_entries WHERE entry_id = '{target_id}';
         END;"
    )).unwrap();
    runtime.clock = Arc::new(|| epoch() + Duration::days(90));
    assert_eq!(recall(&runtime, root.path()).await, vec![]);
    let last_recalled = runtime
        .connection
        .lock()
        .unwrap()
        .query_row(
            "SELECT last_recalled_at FROM memory_entries WHERE entry_id = ?1",
            [target.entry_id.as_str()],
            |row| row.get::<_, Option<String>>(/*idx*/ 0),
        )
        .unwrap();
    assert_eq!(last_recalled, None);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-8 / Entry Lifecycle and Retention.
/// Verifies: fresh evidence for a source-retired claim withholds both incompatible inferred claims.
#[tokio::test]
async fn fresh_retired_evidence_withholds_competing_inference() {
    for change in [SourceChange::Exclude, SourceChange::Delete] {
        let root = tempfile::tempdir().unwrap();
        let runtime = open_runtime(root.path());
        let source = contribute(&runtime, "I prefer tabs.", "indentation", epoch());
        let sources = [devo_protocol::SessionId::try_from(source.session_id.as_str()).unwrap()];
        match change {
            SourceChange::Exclude => runtime.exclude_sources(&sources, epoch() + Duration::days(1)),
            SourceChange::Delete => runtime
                .delete_sources(
                    &sources,
                    epoch() + Duration::days(1),
                    RelatedMemoryDeletion::Preserve,
                )
                .map(|_| ()),
        }
        .unwrap();
        let retired = entries(&runtime).remove(0);
        contribute(
            &runtime,
            "I prefer spaces.",
            "indentation",
            epoch() + Duration::days(2),
        );
        let spaces = entries(&runtime)
            .into_iter()
            .find(|entry| entry.body == "I prefer spaces.")
            .unwrap();
        let fresh = contribute(
            &runtime,
            "I prefer tabs.",
            "indentation",
            epoch() + Duration::days(3),
        );
        assert_eq!(
            runtime.entry_by_id(&retired.entry_id).unwrap(),
            Some(MemoryEntry {
                state: MemoryState::Conflicted,
                updated_at: epoch() + Duration::days(3),
                provenance: vec![devo_protocol::native::rpc_memory::MemoryProvenance {
                    source_session_id: Some(fresh.session_id.to_string()),
                    source_turn_id: Some(fresh.messages[0].turn_id.to_string()),
                    source_user_item_id: Some(fresh.messages[0].item_id.clone()),
                }],
                ..retired
            })
        );
        assert_eq!(
            runtime.entry_by_id(&spaces.entry_id).unwrap(),
            Some(MemoryEntry {
                state: MemoryState::Conflicted,
                ..spaces
            })
        );
        assert_eq!(search(&runtime, /*state*/ None), vec![]);
        assert_eq!(recall(&runtime, root.path()).await, vec![]);
    }
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-10 / Entry Lifecycle and Retention.
/// Verifies: successful on-demand search and read renew valid inference through the injected clock.
#[tokio::test]
async fn on_demand_use_renews_inferred_lifetime() {
    for access in ["read", "search"] {
        let root = tempfile::tempdir().unwrap();
        let mut runtime = open_runtime(root.path());
        contribute(&runtime, "I prefer tabs.", "indentation", epoch());
        let original = entries(&runtime).remove(0);
        runtime.clock = Arc::new(|| epoch() + Duration::days(89));
        if access == "search" {
            assert_eq!(
                search(&runtime, /*state*/ None),
                vec![devo_protocol::native::rpc_memory::MemorySearchEntry {
                    entry_id: original.entry_id.clone(),
                    scope: MemoryScope::User,
                    kind: MemoryKind::Preference,
                    state: MemoryState::Active,
                    summary: "I prefer tabs.".into(),
                }]
            );
        } else {
            assert_eq!(
                runtime
                    .execute_command(MemoryCommand::Read(super::super::ReadMemoryRequest {
                        entry_id: original.entry_id.clone(),
                        workspace_root: root.path().to_path_buf(),
                    }))
                    .await
                    .unwrap(),
                MemoryCommandResult::Read(devo_protocol::native::rpc_memory::MemoryReadEntry {
                    entry_id: original.entry_id.clone(),
                    scope: MemoryScope::User,
                    kind: MemoryKind::Preference,
                    state: MemoryState::Active,
                    body: "I prefer tabs.".into(),
                    source_summary: "Inferred session memory (1 source)".into(),
                })
            );
        }
        runtime.clock = Arc::new(|| epoch() + Duration::days(178));
        assert_eq!(entries(&runtime), vec![original.clone()]);
        runtime.clock = Arc::new(|| epoch() + Duration::days(179));
        assert_eq!(
            entries(&runtime),
            vec![MemoryEntry {
                state: MemoryState::Stale,
                ..original
            }]
        );
    }
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-10 / Entry Lifecycle and Retention.
/// Verifies: on-demand inspection of stale inference never renews its use timestamp or reactivates it.
#[tokio::test]
async fn inactive_on_demand_inspection_does_not_renew_inference() {
    let root = tempfile::tempdir().unwrap();
    let mut runtime = open_runtime(root.path());
    contribute(&runtime, "I prefer tabs.", "indentation", epoch());
    let original = entries(&runtime).remove(0);
    runtime.clock = Arc::new(|| epoch() + Duration::days(90));
    assert_eq!(
        search(&runtime, Some(MemoryState::Stale)),
        vec![devo_protocol::native::rpc_memory::MemorySearchEntry {
            entry_id: original.entry_id.clone(),
            scope: MemoryScope::User,
            kind: MemoryKind::Preference,
            state: MemoryState::Stale,
            summary: "I prefer tabs.".into(),
        }]
    );
    assert_eq!(
        runtime
            .execute_command(MemoryCommand::Read(super::super::ReadMemoryRequest {
                entry_id: original.entry_id.clone(),
                workspace_root: root.path().to_path_buf(),
            }))
            .await
            .unwrap(),
        MemoryCommandResult::Read(devo_protocol::native::rpc_memory::MemoryReadEntry {
            entry_id: original.entry_id.clone(),
            scope: MemoryScope::User,
            kind: MemoryKind::Preference,
            state: MemoryState::Stale,
            body: "I prefer tabs.".into(),
            source_summary: "Inferred session memory (1 source)".into(),
        })
    );
    let last_recalled = runtime
        .connection
        .lock()
        .unwrap()
        .query_row(
            "SELECT last_recalled_at FROM memory_entries WHERE entry_id = ?1",
            [original.entry_id.as_str()],
            |row| row.get::<_, Option<String>>(/*idx*/ 0),
        )
        .unwrap();
    assert_eq!(last_recalled, None);
    assert_eq!(
        entries(&runtime),
        vec![MemoryEntry {
            state: MemoryState::Stale,
            ..original
        }]
    );
}

/// Trace: L2-DES-MEM-001 Rev 4 Failure and Observability.
/// Verifies: an occupied background storage mutex omits recall promptly rather than queuing the foreground turn.
#[tokio::test]
async fn busy_storage_does_not_queue_foreground_recall() {
    let root = tempfile::tempdir().unwrap();
    let runtime = Arc::new(open_runtime(root.path()));
    contribute(&runtime, "I prefer tabs.", "indentation", epoch());
    let healthy = runtime.status().unwrap();
    let (held_rx, release_tx, worker) =
        super::super::contention_test_support::hold_storage(Arc::clone(&runtime));
    held_rx.await.unwrap();
    let result = runtime
        .prepare_turn(PrepareMemoryRequest {
            query: "tabs".into(),
            workspace_root: root.path().to_path_buf(),
            session_recall: MemorySetting::On,
        })
        .await;
    let _ = release_tx.send(());
    worker.join().unwrap();
    assert!(
        matches!(result, Err(super::super::MemoryError::StorageBusy)),
        "{result:?}"
    );
    assert_eq!(runtime.status().unwrap(), healthy);
}
