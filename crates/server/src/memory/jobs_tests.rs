use crate::memory::jobs::JobFailure;
use crate::memory::runtime_test_support::open_runtime;
use crate::memory::source::{ExtractableSource, SourceMessage};
use chrono::{Duration, Utc};
use devo_protocol::native::ids::ItemId;
use devo_protocol::native::ids::{SessionId, TurnId};
use devo_protocol::native::session::MemorySetting;
use pretty_assertions::assert_eq;

fn source() -> ExtractableSource {
    ExtractableSource {
        session_id: SessionId::new(),
        workspace_root: std::path::PathBuf::new(),
        session_contribution: MemorySetting::Inherit,
        observed_at: Utc::now() - Duration::hours(7),
        watermark: "watermark-one".into(),
        legacy_watermark: None,
        messages: vec![SourceMessage {
            turn_id: TurnId::new(),
            item_id: ItemId::new(),
            role: "user".into(),
            observed_at: Utc::now() - Duration::hours(7),
            text: "I prefer tabs".into(),
        }],
    }
}

/// Trace: L2-DES-MEM-001 DD-6, Operational Scheduling
/// Verifies: shared SQLite leases admit exactly one worker per source watermark.
#[test]
fn source_claim_is_exclusive_across_runtime_instances() {
    let root = tempfile::tempdir().unwrap();
    let first = open_runtime(root.path());
    let second = open_runtime(root.path());
    let source = source();
    let now = Utc::now();
    let claim = first.claim_source(&source, now).unwrap();
    assert!(claim.is_some());
    assert_eq!(second.claim_source(&source, now).unwrap(), None);
    assert!(
        second
            .claim_source(&source, now + Duration::minutes(3))
            .unwrap()
            .is_some()
    );
}

/// Trace: L2-DES-MEM-001 DD-6
/// Verifies: off, recent, old, and empty sources never create extraction jobs.
#[test]
fn contribution_and_idle_window_gate_claims() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let now = Utc::now();
    for (setting, age, empty) in [
        (MemorySetting::Off, 7, false),
        (MemorySetting::On, 5, false),
        (MemorySetting::On, 31 * 24, false),
        (MemorySetting::On, 7, true),
    ] {
        let mut source = source();
        source.session_contribution = setting;
        source.observed_at = now - Duration::hours(age);
        if empty {
            source.messages.clear();
        }
        assert_eq!(runtime.claim_source(&source, now).unwrap(), None);
    }
    assert_eq!(runtime.status().unwrap().pending_job_count, 0);
}

/// Trace: L2-DES-MEM-001 Operational Scheduling
/// Verifies: transient jobs wait exponentially and terminate after three claims.
#[test]
fn transient_jobs_have_bounded_exponential_retries() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let source = source();
    let now = Utc::now();
    let first = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime
        .fail_job(&first, JobFailure::TransientProvider, now)
        .unwrap();
    assert_eq!(
        runtime
            .claim_source(&source, now + Duration::seconds(29))
            .unwrap(),
        None
    );
    let second = runtime
        .claim_source(&source, now + Duration::seconds(30))
        .unwrap()
        .unwrap();
    runtime
        .fail_job(
            &second,
            JobFailure::TransientProvider,
            now + Duration::seconds(30),
        )
        .unwrap();
    assert_eq!(
        runtime
            .claim_source(&source, now + Duration::seconds(89))
            .unwrap(),
        None
    );
    let third = runtime
        .claim_source(&source, now + Duration::seconds(90))
        .unwrap()
        .unwrap();
    runtime
        .fail_job(
            &third,
            JobFailure::TransientProvider,
            now + Duration::seconds(90),
        )
        .unwrap();
    assert_eq!(
        runtime
            .claim_source(&source, now + Duration::days(1))
            .unwrap(),
        None
    );
    let status = runtime.status().unwrap();
    assert_eq!(
        (
            status.retrying_job_count,
            status.error_job_count,
            status.error_classes
        ),
        (0, 1, vec!["transient_provider_error".to_string()])
    );
}

/// Trace: L2-DES-MEM-001 Operational Scheduling
/// Verifies: an expired worker cannot overwrite the newer lease owner's result.
#[test]
fn stale_worker_cannot_fail_a_reclaimed_job() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let source = source();
    let now = Utc::now();
    let old = runtime.claim_source(&source, now).unwrap().unwrap();
    let new = runtime
        .claim_source(&source, now + Duration::minutes(3))
        .unwrap()
        .unwrap();
    runtime
        .fail_job(
            &old,
            JobFailure::PermanentProvider,
            now + Duration::minutes(3),
        )
        .unwrap();
    runtime
        .fail_job(&new, JobFailure::InvalidOutput, now + Duration::minutes(3))
        .unwrap();
    assert_eq!(
        runtime.status().unwrap().error_classes,
        vec!["invalid_structured_output"]
    );
}

/// Trace: L2-DES-MEM-001 Operational Scheduling
/// Verifies: declined pre-call quota/activity admission releases its attempt for a later eligible scan.
#[test]
fn released_claim_does_not_consume_provider_attempt() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let source = source();
    let now = Utc::now();
    let claim = runtime.claim_source(&source, now).unwrap().unwrap();
    runtime.release_job(&claim).unwrap();
    let next = runtime.claim_source(&source, now).unwrap().unwrap();
    assert_eq!(next.attempt, 1);
    assert_ne!(next.owner, claim.owner);
}

/// Trace: L2-DES-MEM-001 Operational Scheduling
/// Verifies: truly simultaneous SQLite claims across instances yield one provider attempt.
#[test]
fn concurrent_instances_claim_once() {
    let root = tempfile::tempdir().unwrap();
    let first = open_runtime(root.path());
    let second = open_runtime(root.path());
    let source = source();
    let now = Utc::now();
    let barrier = std::sync::Barrier::new(2);
    let claims = std::thread::scope(|scope| {
        let left = scope.spawn(|| {
            barrier.wait();
            first.claim_source(&source, now).unwrap()
        });
        let right = scope.spawn(|| {
            barrier.wait();
            second.claim_source(&source, now).unwrap()
        });
        [left.join().unwrap(), right.join().unwrap()]
    });
    assert_eq!(claims.into_iter().flatten().count(), 1);
}
