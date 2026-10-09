use std::sync::Arc;

use chrono::Duration;
use pretty_assertions::assert_eq;

use super::super::{MEMORY_DATABASE_FILENAME, MemoryRuntime, schema};
use super::{contribute, epoch, open_runtime};

/// Trace: L2-DES-MEM-001 Storage Model / Failure and Observability.
/// Verifies: only source scans advance status across exact retention boundaries and restart.
#[test]
fn scan_time_excludes_maintenance_before_and_after_retention() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let source = contribute(&runtime, "I prefer tabs.", "indentation", epoch());
    runtime
        .connection
        .lock()
        .unwrap()
        .execute_batch(
            "INSERT INTO memory_jobs (
            job_id, job_kind, job_key, source_session_id, source_watermark,
            state, created_at, updated_at
         ) VALUES ('maintenance', 'projection_rebuild', 'maintenance', 'maintenance', 'done',
            'completed', '2030-01-01T02:00:00Z', '2030-01-01T02:00:00Z');",
        )
        .unwrap();
    let mut expected = runtime.status().unwrap();
    assert_eq!(expected.last_successful_scan_at, Some(epoch()));

    for now in [
        epoch() + Duration::days(30) - Duration::milliseconds(1),
        epoch() + Duration::days(30),
        epoch() + Duration::days(30) + Duration::hours(2),
    ] {
        runtime.prune_expired(now).unwrap();
        if now >= epoch() + Duration::days(30) {
            expected.candidate_count = 0;
        }
        assert_eq!(runtime.status().unwrap(), expected);
    }
    let counts: (i64, i64, i64) = runtime
        .connection
        .lock()
        .unwrap()
        .query_row(
            "SELECT (SELECT COUNT(*) FROM memory_jobs),
                (SELECT COUNT(*) FROM memory_candidates),
                (SELECT COUNT(*) FROM memory_job_receipts)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(counts, (0, 0, 2));
    drop(runtime);

    let reopened = MemoryRuntime::open_with_clock(
        root.path().to_path_buf(),
        devo_core::MemoryConfig {
            enabled: true,
            ..Default::default()
        },
        Arc::new(|| epoch() + Duration::days(30) + Duration::hours(2)),
    )
    .unwrap();
    assert_eq!(reopened.status().unwrap(), expected);
    assert_eq!(reopened.claim_source(&source, epoch()).unwrap(), None);
}

/// Trace: L2-DES-MEM-001 Storage Model / Failure and Observability.
/// Verifies: maintenance alone never produces a successful source scan timestamp.
#[test]
fn maintenance_only_has_no_scan_time_after_retention_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let expected = runtime.status().unwrap();
    runtime
        .connection
        .lock()
        .unwrap()
        .execute_batch(
            "INSERT INTO memory_jobs (
            job_id, job_kind, job_key, source_session_id, source_watermark,
            state, created_at, updated_at
         ) VALUES ('maintenance', 'projection_rebuild', 'maintenance', 'maintenance', 'done',
            'completed', '2030-01-01T00:00:00Z', '2030-01-01T00:00:00Z');",
        )
        .unwrap();
    assert_eq!(runtime.status().unwrap(), expected);
    runtime.prune_expired(epoch() + Duration::days(30)).unwrap();
    assert_eq!(runtime.status().unwrap(), expected);
    drop(runtime);
    assert_eq!(open_runtime(root.path()).status().unwrap(), expected);
}

/// Trace: L2-DES-MEM-001 Storage Model / Failure and Observability.
/// Verifies: v9 receipts retain deduplication without inventing scan attribution or masking known scans.
#[test]
fn legacy_receipts_stay_unknown_and_keep_watermark_idempotent() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    let source = contribute(&runtime, "I prefer tabs.", "indentation", epoch());
    let mut expected = runtime.status().unwrap();
    expected.last_successful_scan_at = None;
    {
        let connection = runtime.connection.lock().unwrap();
        connection
            .execute_batch(
                "DROP TABLE memory_job_receipts;
             CREATE TABLE memory_job_receipts (
                 source_session_id TEXT NOT NULL,
                 source_watermark TEXT NOT NULL,
                 completed_at TEXT NOT NULL,
                 PRIMARY KEY(source_session_id, source_watermark)
             );
             UPDATE memory_schema_meta SET value = '9' WHERE key = 'schema_version';
             DELETE FROM memory_jobs;",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO memory_job_receipts VALUES (?1, ?2, '2030-01-01T02:00:00Z')",
                rusqlite::params![source.session_id.as_str(), source.watermark],
            )
            .unwrap();
    }
    drop(runtime);

    let migrated = open_runtime(root.path());
    assert_eq!(migrated.status().unwrap(), expected);
    assert_eq!(migrated.claim_source(&source, epoch()).unwrap(), None);
    contribute(&migrated, "I prefer tabs.", "indentation", epoch());
    expected.last_successful_scan_at = Some(epoch());
    expected.candidate_count = 2;
    assert_eq!(migrated.status().unwrap(), expected);
    migrated
        .prune_expired(epoch() + Duration::days(30))
        .unwrap();
    expected.candidate_count = 0;
    assert_eq!(migrated.status().unwrap(), expected);
    drop(migrated);

    let reopened = open_runtime(root.path());
    assert_eq!(reopened.status().unwrap(), expected);
    assert_eq!(reopened.claim_source(&source, epoch()).unwrap(), None);
}

/// Trace: L2-DES-MEM-001 Storage Model.
/// Verifies: failed receipt migration rolls back schema and data and can be retried.
#[test]
fn receipt_kind_migration_rolls_back_and_retries() {
    let root = tempfile::tempdir().unwrap();
    drop(open_runtime(root.path()));
    let connection =
        rusqlite::Connection::open(root.path().join(MEMORY_DATABASE_FILENAME)).unwrap();
    connection
        .execute_batch(
            "DROP TABLE memory_job_receipts;
         CREATE TABLE memory_job_receipts (
             source_session_id TEXT NOT NULL,
             source_watermark TEXT NOT NULL,
             completed_at TEXT NOT NULL,
             PRIMARY KEY(source_session_id, source_watermark)
         );
         INSERT INTO memory_job_receipts VALUES ('legacy', 'done', '2030-01-01T02:00:00Z');
         UPDATE memory_schema_meta SET value = '9' WHERE key = 'schema_version';
         CREATE TRIGGER reject_receipt_version BEFORE UPDATE ON memory_schema_meta
         WHEN NEW.value = '11' BEGIN SELECT RAISE(ABORT, 'injected'); END;",
        )
        .unwrap();
    assert!(schema::create_schema(&connection).is_err());
    let snapshot: (String, bool, i64) = connection.query_row(
        "SELECT (SELECT value FROM memory_schema_meta WHERE key = 'schema_version'),
                EXISTS(SELECT 1 FROM pragma_table_info('memory_job_receipts') WHERE name = 'job_kind'),
                (SELECT COUNT(*) FROM memory_job_receipts)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).unwrap();
    assert_eq!(snapshot, ("9".into(), false, 1));
    connection
        .execute_batch("DROP TRIGGER reject_receipt_version")
        .unwrap();
    schema::create_schema(&connection).unwrap();
    schema::create_schema(&connection).unwrap();
    let receipt: (String, String, String, Option<String>) = connection.query_row(
        "SELECT source_session_id, source_watermark, completed_at, job_kind FROM memory_job_receipts",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    ).unwrap();
    assert_eq!(
        receipt,
        (
            "legacy".into(),
            "done".into(),
            "2030-01-01T02:00:00Z".into(),
            None
        )
    );
}
