use super::extraction::ExtractionCandidate;
use super::jobs::SourceJobAction;
use super::rebuild::ScanTarget;
use super::runtime_test_support::{forget_request, prepare_forget};
use super::source::{ExtractableSource, SourceMessage};
use super::{
    ListMemoryRequest, MemoryCommand, MemoryForgetSelector, MemoryRuntime, ScopedMemoryRequest,
};
use chrono::{DateTime, Duration, Utc};
use devo_core::MemoryConfig;
use devo_protocol::native::ids::{ItemId, SessionId, TurnId};
use devo_protocol::native::rpc_memory::{MemoryKind, MemoryScope, MemoryState};
use devo_protocol::native::session::MemorySetting;
use pretty_assertions::assert_eq;
use std::path::Path;
use std::sync::Arc;

fn epoch() -> DateTime<Utc> {
    "2030-01-01T00:00:00Z".parse().unwrap()
}
fn open(root: &Path) -> MemoryRuntime {
    MemoryRuntime::open_with_clock(
        root.into(),
        MemoryConfig {
            enabled: true,
            ..Default::default()
        },
        Arc::new(epoch),
    )
    .unwrap()
}
fn request(scope: MemoryScope, workspace: &Path) -> ScopedMemoryRequest {
    ScopedMemoryRequest {
        scope,
        workspace_root: workspace.into(),
    }
}
fn fixture(workspace: &Path) -> (ExtractableSource, ExtractionCandidate) {
    let turn = TurnId::new();
    let source = ExtractableSource {
        session_id: SessionId::from_string(super::SessionId::new().to_string()),
        workspace_root: workspace.into(),
        session_contribution: MemorySetting::On,
        observed_at: epoch() - Duration::days(60),
        watermark: "retained-history".into(),
        legacy_watermarks: Vec::new(),
        messages: vec![SourceMessage {
            turn_id: turn.clone(),
            item_id: ItemId::new(),
            observed_at: epoch() - Duration::days(60),
            role: "user".into(),
            text: "Use tabs".into(),
        }],
    };
    let candidate = ExtractionCandidate {
        scope: MemoryScope::User,
        kind: MemoryKind::Preference,
        key: "indentation".into(),
        body: "Use tabs".into(),
        evidence: vec![turn],
    };
    (source, candidate)
}

/// Trace: L2-DES-MEM-001 DD-9, DD-12. Rebuild leases survive crashes, are scoped, and cannot duplicate source receipts.
#[test]
fn rebuild_jobs_are_scoped_idempotent_and_recover_expired_leases() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("project");
    std::fs::create_dir(&workspace).unwrap();
    let memory = open(root.path());
    let (source, candidate) = fixture(&workspace);
    assert_eq!(memory.claim_source(&source, epoch()).unwrap(), None);
    memory
        .begin_rebuild(request(MemoryScope::User, &workspace))
        .unwrap();
    let target = ScanTarget::Rebuild(memory.pending_rebuilds().unwrap().remove(0));
    let first = memory
        .source_job(&source, epoch(), &target, SourceJobAction::Claim)
        .unwrap()
        .unwrap();
    let concurrent = open(root.path());
    assert_eq!(
        concurrent
            .source_job(&source, epoch(), &target, SourceJobAction::Claim)
            .unwrap(),
        None
    );
    drop(concurrent);
    drop(memory);
    let memory = open(root.path());
    let now = epoch() + Duration::minutes(3);
    let recovered = memory
        .source_job(&source, now, &target, SourceJobAction::Claim)
        .unwrap()
        .unwrap();
    memory
        .commit_extraction(&first, &source, std::slice::from_ref(&candidate), now)
        .unwrap();
    assert_eq!(
        memory.list(ListMemoryRequest::default()).unwrap().data,
        vec![]
    );
    memory
        .commit_extraction(&recovered, &source, std::slice::from_ref(&candidate), now)
        .unwrap();
    let entries = memory.list(ListMemoryRequest::default()).unwrap().data;
    assert_eq!(entries.len(), 1);
    memory
        .commit_extraction(&recovered, &source, std::slice::from_ref(&candidate), now)
        .unwrap();
    assert_eq!(
        memory.list(ListMemoryRequest::default()).unwrap().data,
        entries
    );
    memory.prune_expired(now + Duration::days(31)).unwrap();
    assert_eq!(
        memory
            .source_job(
                &source,
                now + Duration::days(31),
                &target,
                SourceJobAction::Claim
            )
            .unwrap(),
        None
    );
    // A separate scope has its own job and can never write back into User.
    memory
        .begin_rebuild(request(MemoryScope::Project, &workspace))
        .unwrap();
    let target = ScanTarget::Rebuild(
        memory
            .pending_rebuilds()
            .unwrap()
            .into_iter()
            .find(|r| r.scope == MemoryScope::Project)
            .unwrap(),
    );
    let claim = memory
        .source_job(&source, now, &target, SourceJobAction::Claim)
        .unwrap()
        .unwrap();
    memory
        .commit_extraction(
            &claim,
            &source,
            &[
                candidate.clone(),
                ExtractionCandidate {
                    scope: MemoryScope::Project,
                    ..candidate
                },
            ],
            now,
        )
        .unwrap();
    assert_eq!(
        memory.list(ListMemoryRequest::default()).unwrap().data,
        entries
    );
    assert_eq!(
        memory
            .list(ListMemoryRequest {
                scope: Some(MemoryScope::Project),
                workspace_root: workspace,
                ..Default::default()
            })
            .unwrap()
            .data
            .len(),
        1
    );
}

/// Trace: L2-DES-MEM-001 DD-9. Reset cancels old rebuild workers even if the injected clock has not advanced.
#[test]
fn another_reset_cancels_inflight_rebuild_and_opens_a_new_epoch() {
    let root = tempfile::tempdir().unwrap();
    let memory = open(root.path());
    let (source, candidate) = fixture(root.path());
    let first = memory
        .begin_rebuild(request(MemoryScope::User, root.path()))
        .unwrap();
    let target = ScanTarget::Rebuild(memory.pending_rebuilds().unwrap().remove(0));
    let claim = memory
        .source_job(&source, epoch(), &target, SourceJobAction::Claim)
        .unwrap()
        .unwrap();
    memory
        .reset(request(MemoryScope::User, root.path()))
        .unwrap();
    memory
        .commit_extraction(&claim, &source, std::slice::from_ref(&candidate), epoch())
        .unwrap();
    assert_eq!(
        memory.list(ListMemoryRequest::default()).unwrap().data,
        vec![]
    );
    assert_eq!(
        memory
            .source_job(&source, epoch(), &target, SourceJobAction::Claim)
            .unwrap(),
        None
    );
    let second = memory
        .begin_rebuild(request(MemoryScope::User, root.path()))
        .unwrap();
    assert_ne!(first.rebuild_id, second.rebuild_id);
    let target = ScanTarget::Rebuild(memory.pending_rebuilds().unwrap().remove(0));
    let claim = memory
        .source_job(&source, epoch(), &target, SourceJobAction::Claim)
        .unwrap()
        .unwrap();
    memory
        .commit_extraction(&claim, &source, std::slice::from_ref(&candidate), epoch())
        .unwrap();
    memory
        .reset(request(MemoryScope::User, root.path()))
        .unwrap();
    let third = memory
        .begin_rebuild(request(MemoryScope::User, root.path()))
        .unwrap();
    assert_ne!(second.rebuild_id, third.rebuild_id);
    let target = ScanTarget::Rebuild(memory.pending_rebuilds().unwrap().remove(0));
    let claim = memory
        .source_job(&source, epoch(), &target, SourceJobAction::Claim)
        .unwrap()
        .unwrap();
    memory
        .commit_extraction(&claim, &source, &[candidate], epoch())
        .unwrap();
    assert_eq!(
        memory
            .list(ListMemoryRequest::default())
            .unwrap()
            .data
            .len(),
        1
    );
}

/// Trace: L2-DES-MEM-001 DD-6, DD-8, DD-9. Rebuild reuses revocation, credential rejection, validation and inferred conflict admission.
#[tokio::test]
async fn rebuild_preserves_revocations_redaction_and_conflicts() {
    let root = tempfile::tempdir().unwrap();
    let memory = open(root.path());
    let (mut source, candidate) = fixture(root.path());
    memory
        .begin_rebuild(request(MemoryScope::User, root.path()))
        .unwrap();
    let target = ScanTarget::Rebuild(memory.pending_rebuilds().unwrap().remove(0));
    let first = memory
        .source_job(&source, epoch(), &target, SourceJobAction::Claim)
        .unwrap()
        .unwrap();
    memory
        .commit_extraction(&first, &source, std::slice::from_ref(&candidate), epoch())
        .unwrap();
    let entry = memory
        .list(ListMemoryRequest::default())
        .unwrap()
        .data
        .remove(0);
    let forget = prepare_forget(
        &memory,
        forget_request(MemoryForgetSelector::EntryId(entry.entry_id.clone())),
    )
    .await
    .unwrap();
    memory
        .execute_command(MemoryCommand::Forget(forget))
        .await
        .unwrap();
    let revoked = memory.list(ListMemoryRequest::default()).unwrap().data;
    source.watermark = "after-revocation".into();
    let claim = memory
        .source_job(&source, epoch(), &target, SourceJobAction::Claim)
        .unwrap()
        .unwrap();
    memory
        .commit_extraction(
            &claim,
            &source,
            &[
                candidate.clone(),
                ExtractionCandidate {
                    key: "credential".into(),
                    body: "api_key=sk-1234567890abcdef1234567890abcdef".into(),
                    ..candidate.clone()
                },
            ],
            epoch(),
        )
        .unwrap();
    assert_eq!(
        memory.list(ListMemoryRequest::default()).unwrap().data,
        revoked
    );
    // An independent live claim remains conflicted when rebuilt history opposes it.
    source.session_id = SessionId::new();
    source.watermark = "inferred-a".into();
    let candidate = ExtractionCandidate {
        key: "theme".into(),
        body: "Use dark theme".into(),
        ..candidate
    };
    let claim = memory
        .source_job(&source, epoch(), &target, SourceJobAction::Claim)
        .unwrap()
        .unwrap();
    memory
        .commit_extraction(&claim, &source, std::slice::from_ref(&candidate), epoch())
        .unwrap();
    let accepted = memory
        .list(ListMemoryRequest {
            text: Some("dark theme".into()),
            ..Default::default()
        })
        .unwrap()
        .data
        .remove(0);
    source.session_id = SessionId::new();
    source.watermark = "inferred-b".into();
    let claim = memory
        .source_job(&source, epoch(), &target, SourceJobAction::Claim)
        .unwrap()
        .unwrap();
    memory
        .commit_extraction(
            &claim,
            &source,
            &[ExtractionCandidate {
                body: "Use light theme".into(),
                ..candidate
            }],
            epoch(),
        )
        .unwrap();
    assert_eq!(
        memory
            .list(ListMemoryRequest {
                text: Some("dark theme".into()),
                ..Default::default()
            })
            .unwrap()
            .data,
        vec![devo_protocol::native::rpc_memory::MemoryEntry {
            state: MemoryState::Conflicted,
            ..accepted
        }]
    );
}

/// Trace: L2-DES-MEM-001 DD-6, DD-9. Rebuild queues work without spending attempts and honors contribution and durable source fences.
#[test]
fn rebuild_queue_obeys_current_source_controls() {
    let root = tempfile::tempdir().unwrap();
    let memory = open(root.path());
    let (mut source, _) = fixture(root.path());
    memory
        .begin_rebuild(request(MemoryScope::User, root.path()))
        .unwrap();
    let target = ScanTarget::Rebuild(memory.pending_rebuilds().unwrap().remove(0));
    memory
        .source_job(&source, epoch(), &target, SourceJobAction::Queue)
        .unwrap();
    assert_eq!(memory.status().unwrap().pending_job_count, 1);
    source.session_contribution = MemorySetting::Off;
    assert_eq!(
        memory
            .source_job(&source, epoch(), &target, SourceJobAction::Claim)
            .unwrap(),
        None
    );
    source.session_contribution = MemorySetting::On;
    memory
        .exclude_sources(
            &[super::SessionId::try_from(source.session_id.as_str()).unwrap()],
            epoch(),
        )
        .unwrap();
    assert_eq!(
        memory
            .source_job(&source, epoch(), &target, SourceJobAction::Claim)
            .unwrap(),
        None
    );
}

/// Trace: L2-DES-MEM-001 DD-9. Deliberate rebuild must not resurrect a forgotten identity after its scope is reset.
#[tokio::test]
async fn forget_then_reset_then_rebuild_keeps_revoked_history_blocked() {
    let root = tempfile::tempdir().unwrap();
    let memory = open(root.path());
    let (source, candidate) = fixture(root.path());
    memory
        .begin_rebuild(request(MemoryScope::User, root.path()))
        .unwrap();
    let target = ScanTarget::Rebuild(memory.pending_rebuilds().unwrap().remove(0));
    let claim = memory
        .source_job(&source, epoch(), &target, SourceJobAction::Claim)
        .unwrap()
        .unwrap();
    memory
        .commit_extraction(&claim, &source, std::slice::from_ref(&candidate), epoch())
        .unwrap();
    let entry = memory
        .list(ListMemoryRequest::default())
        .unwrap()
        .data
        .remove(0);
    let forget = prepare_forget(
        &memory,
        forget_request(MemoryForgetSelector::EntryId(entry.entry_id)),
    )
    .await
    .unwrap();
    memory
        .execute_command(MemoryCommand::Forget(forget))
        .await
        .unwrap();
    memory
        .reset(request(MemoryScope::User, root.path()))
        .unwrap();
    memory
        .begin_rebuild(request(MemoryScope::User, root.path()))
        .unwrap();
    let target = ScanTarget::Rebuild(memory.pending_rebuilds().unwrap().remove(0));
    let claim = memory
        .source_job(&source, epoch(), &target, SourceJobAction::Claim)
        .unwrap()
        .unwrap();
    memory
        .commit_extraction(&claim, &source, &[candidate], epoch())
        .unwrap();
    assert_eq!(
        memory.list(ListMemoryRequest::default()).unwrap().data,
        vec![]
    );
}

/// Trace: L2-DES-MEM-001 DD-9. A retained receipt must still cancel a queued superseded watermark.
#[test]
fn rebuild_receipt_cancels_superseded_queued_watermark() {
    let root = tempfile::tempdir().unwrap();
    let memory = open(root.path());
    let (mut source, candidate) = fixture(root.path());
    memory
        .begin_rebuild(request(MemoryScope::User, root.path()))
        .unwrap();
    let target = ScanTarget::Rebuild(memory.pending_rebuilds().unwrap().remove(0));
    let claim = memory
        .source_job(&source, epoch(), &target, SourceJobAction::Claim)
        .unwrap()
        .unwrap();
    memory
        .commit_extraction(&claim, &source, &[candidate], epoch())
        .unwrap();
    let now = epoch() + Duration::days(31);
    memory.prune_expired(now).unwrap();
    let completed = source.watermark.clone();
    source.watermark = "superseded".into();
    memory
        .source_job(&source, now, &target, SourceJobAction::Queue)
        .unwrap();
    source.watermark = completed;
    assert_eq!(
        memory
            .source_job(&source, now, &target, SourceJobAction::Claim)
            .unwrap(),
        None
    );
    assert_eq!(
        memory.status().unwrap().rebuild.unwrap().pending_job_count,
        0
    );
}

/// Trace: L2-DES-MEM-001 DD-9, DD-12. Reset retains the proven identity of legacy inferred tombstones across restart.
#[tokio::test]
async fn legacy_revocation_survives_reset_restart_and_rebuild() {
    let root = tempfile::tempdir().unwrap();
    let memory = open(root.path());
    let (source, candidate) = fixture(root.path());
    memory
        .begin_rebuild(request(MemoryScope::User, root.path()))
        .unwrap();
    let target = ScanTarget::Rebuild(memory.pending_rebuilds().unwrap().remove(0));
    let claim = memory
        .source_job(&source, epoch(), &target, SourceJobAction::Claim)
        .unwrap()
        .unwrap();
    memory
        .commit_extraction(&claim, &source, std::slice::from_ref(&candidate), epoch())
        .unwrap();
    let entry = memory
        .list(ListMemoryRequest::default())
        .unwrap()
        .data
        .remove(0);
    let forget = prepare_forget(
        &memory,
        forget_request(MemoryForgetSelector::EntryId(entry.entry_id)),
    )
    .await
    .unwrap();
    memory
        .execute_command(MemoryCommand::Forget(forget))
        .await
        .unwrap();
    memory
        .connection
        .lock()
        .unwrap()
        .execute_batch(
            "UPDATE memory_entries SET normalized_key = 'use tabs';
        UPDATE memory_revocations SET normalized_key = 'use tabs';",
        )
        .unwrap();
    let revoked_at: String = memory
        .connection
        .lock()
        .unwrap()
        .query_row("SELECT revoked_at FROM memory_revocations", [], |row| {
            row.get(0)
        })
        .unwrap();
    memory
        .reset(request(MemoryScope::User, root.path()))
        .unwrap();
    drop(memory);
    let memory = open(root.path());
    memory
        .begin_rebuild(request(MemoryScope::User, root.path()))
        .unwrap();
    let target = ScanTarget::Rebuild(memory.pending_rebuilds().unwrap().remove(0));
    let claim = memory
        .source_job(&source, epoch(), &target, SourceJobAction::Claim)
        .unwrap()
        .unwrap();
    memory
        .commit_extraction(&claim, &source, &[candidate], epoch())
        .unwrap();
    assert_eq!(
        memory.list(ListMemoryRequest::default()).unwrap().data,
        vec![]
    );
    let tombstones = memory.connection.lock().unwrap().prepare("SELECT normalized_key, revoked_at, restored_at FROM memory_revocations ORDER BY normalized_key").unwrap()
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, Option<String>>(2)?))).unwrap()
        .collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(tombstones, vec![("Use tabs".into(), revoked_at, None)]);
}

/// Trace: L2-DES-MEM-001 DD-9, DD-12. Late workers preserve recovery under both enqueue/finish orderings.
#[test]
fn late_rebuild_queue_reopens_completed_authorization_for_restart() {
    let root = tempfile::tempdir().unwrap();
    let memory = open(root.path());
    let (mut source, _) = fixture(root.path());
    memory
        .begin_rebuild(request(MemoryScope::User, root.path()))
        .unwrap();
    let request = memory.pending_rebuilds().unwrap().remove(0);
    let target = ScanTarget::Rebuild(request.clone());
    let old = memory
        .source_job(&source, epoch(), &target, SourceJobAction::Claim)
        .unwrap()
        .unwrap();
    memory
        .commit_extraction(&old, &source, &[], epoch())
        .unwrap();
    assert!(
        memory
            .finish_rebuild_pass(&request, /*deferred*/ false)
            .unwrap()
    );
    assert_eq!(memory.pending_rebuilds().unwrap(), vec![]);
    source.watermark = "late-history".into();
    let late_worker = open(root.path());
    late_worker
        .source_job(&source, epoch(), &target, SourceJobAction::Queue)
        .unwrap();
    drop(late_worker);
    drop(memory);
    let recovered = open(root.path());
    assert_eq!(recovered.pending_rebuilds().unwrap(), vec![request.clone()]);
    assert!(
        !recovered
            .finish_rebuild_pass(&request, /*deferred*/ false)
            .unwrap()
    );
    assert_eq!(
        recovered.status().unwrap().rebuild,
        Some(devo_protocol::native::rpc_memory::MemoryRebuildStatus {
            pending_request_count: 1,
            pending_job_count: 1,
            completed_job_count: 1,
            ..Default::default()
        })
    );
    let claim = recovered
        .source_job(&source, epoch(), &target, SourceJobAction::Claim)
        .unwrap()
        .unwrap();
    recovered
        .commit_extraction(&claim, &source, &[], epoch())
        .unwrap();
    assert!(
        recovered
            .finish_rebuild_pass(&request, /*deferred*/ false)
            .unwrap()
    );
    assert_eq!(recovered.pending_rebuilds().unwrap(), vec![]);
}
